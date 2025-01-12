from arrival_times import ArrivalTime
import datetime
import gc
import gtfs_kit as gk
import json
import os
import pandas as pd
import queue
import refresh_feed
import requests
import sys
import time
import zipfile

class GTFSClient:
    def __init__(self, feed_url: str, gtfs_r_url: str, gtfs_r_api_key: str, 
                 stop_codes: list[str], routes_for_stops: dict[str, str],
                 update_queue: queue.Queue, update_interval_seconds: int = 60):

        self.stop_codes = stop_codes
        self.routes_for_stops = routes_for_stops

        feed_name = '/tmp/' + feed_url.split('/')[-1]
        self.gtfs_r_url = gtfs_r_url
        self.gtfs_r_api_key = gtfs_r_api_key

        # Make sure that the feed file is up to date
        try:
            last_mtime = int(os.stat(feed_name).st_mtime)
        except:
            last_mtime = 0

        _, new_mtime = refresh_feed.update_local_file_from_url_v1(last_mtime, feed_name, feed_url)

        # Load the feed
        self.feed = self._read_feed(feed_name, dist_units='km')
        gc.collect()
        self.stop_ids = self.__wanted_stop_ids()
        self.deltas = {}
        self.canceled_trips = set()
        self.added_stops = []

        # Schedule refresh       
        self._update_queue = update_queue
        if update_interval_seconds and update_queue: 
            self._update_interval_seconds = update_interval_seconds

    def _read_feed(self, path: str, dist_units: str) -> gk.Feed:
        """
        NOTE: This helper method was extracted from gtfs_kit.feed to modify it
        to only load the stop_times for the stops we are interested in,
        because loading the entire feed would use more memory than the Raspberry Pi Zero W has.

        This version also reads CSV data straight from the zip file to avoid
        wearing out the Pi's SD card.
        """
        files_to_load = [
            # List of feed files to load. stop_times.txt is loaded separately.
            'trips.txt',
            'routes.txt',
            'calendar.txt',
            'calendar_dates.txt',
            'stops.txt',
            'agency.txt'
        ]

        if not os.path.exists(path):
            raise ValueError("Path {} does not exist".format(path))

        print("Loading GTFS feed {}".format(path), file=sys.stderr)
        gc.collect()

        feed_dict = {table: None for table in gk.cs.GTFS_REF["table"]}
        with zipfile.ZipFile(path) as z:
            for filename in files_to_load:
                table = filename.split(".")[0]
                # read the file
                with z.open(filename) as f:
                    df = pd.read_csv(f, dtype=gk.cs.DTYPE, encoding="utf-8-sig")
                    if not df.empty:
                        feed_dict[table] = gk.cn.clean_column_names(df)

                    gc.collect()

            # Finally, load stop_times.txt
            # Obtain the list of IDs of the desired stops. This is similar to what __wanted_stop_ids() does, 
            # but without a dependency on a fully formed feed object
            wanted_stop_ids = feed_dict.get("stops")[feed_dict.get("stops")["stop_code"].isin(self.stop_codes)]["stop_id"]
            with z.open("stop_times.txt") as f:
                iter_csv = pd.read_csv(f, iterator=True, chunksize=1000, dtype=gk.cs.DTYPE, encoding="utf-8-sig")
                df = pd.concat([chunk[chunk["stop_id"].isin(wanted_stop_ids)] for chunk in iter_csv])

            gc.collect()

            if not df.empty:
                # Fix arrival and departure times so that comparisons work the way they are expected to
                df["arrival_time"] = df.apply(lambda row: row["arrival_time"] if len(row["arrival_time"]) == 8 else "0"+row["arrival_time"], axis=1)
                gc.collect()
                df["departure_time"] = df.apply(lambda row: row["departure_time"] if len(row["departure_time"]) == 8 else "0"+row["departure_time"], axis=1)
                gc.collect()
                feed_dict["stop_times"] = gk.cn.clean_column_names(df)
                gc.collect()

        feed_dict["dist_units"] = dist_units

        # Create feed
        return gk.Feed(**feed_dict)


    def __wanted_stop_ids(self) -> pd.core.frame.DataFrame:
        """
        Return a DataFrame with the ID and names of the chosen stop(s) as requested in station_names
        """
        stops = self.feed.stops[self.feed.stops["stop_code"].isin(self.stop_codes)]
        if stops.empty: 
            raise Exception("Stops is empty!")
        return stops["stop_id"]


    def __service_ids_active_at(self, when: datetime) -> pd.core.frame.DataFrame:
        """
        Returns the service IDs active at a particular point in time
        """
        todays_date = when.strftime("%Y%m%d")
        todays_weekday = when.strftime("%A").lower()
        active_calendars = self.feed.calendar.query('start_date <= @todays_date and end_date >= @todays_date and {} == 1'.format(todays_weekday))
        return active_calendars


    def __current_calendars(self) -> pd.core.frame.DataFrame:
        """
        Filter the calendar entries to find all services that apply for today.
        Returns an empty list if none do.
        """
        
        # Take the service IDs active today
        now = datetime.datetime.now()
        now_active = self.__service_ids_active_at(now)
        if now_active.empty:
            print("There are no service IDs for today!")

        # Merge with the service IDs for tomorrow (in case the number of trips spills over to tomorrow)
        tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
        tomorrow_active = self.__service_ids_active_at(tomorrow)
        if tomorrow_active.empty:
            print("There are no service IDs for tomorrow!")

        #active_calendars = pd.concat([now_active, tomorrow_active])
        active_calendars = now_active
        if active_calendars.empty:
            print("The concatenation of today and tomorrow's calendars is empty. This should not happen.")

        return active_calendars


    def __current_service_ids(self) -> pd.core.series.Series:
        """
        Filter the calendar entries to find all service ids that apply for today.
        Returns an empty list if none do.
        """
        return self.__current_calendars()["service_id"]


    def __trip_ids_for_service_ids(self, service_ids: pd.core.series.Series) -> pd.core.series.Series:
        """
        Returns a dataframe with the trip IDs for the given service IDs 
        """
        trips = self.feed.trips[self.feed.trips["service_id"].isin(service_ids)]
        if trips.empty:
            print("There are no active trips!")

        return trips["trip_id"]


    def __next_n_buses(self, 
                    trip_ids: pd.core.series.Series,
                    n: int) -> pd.core.frame.DataFrame:
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")
        next_stops = self.feed.stop_times[self.feed.stop_times["stop_id"].isin(self.stop_ids)
                                    & self.feed.stop_times["trip_id"].isin(trip_ids)
                                    & (self.feed.stop_times["arrival_time"] > current_time)]
        next_stops = next_stops.sort_values("arrival_time")
        return next_stops[:n][["trip_id", "arrival_time", "stop_id"]]


    def __join_data(self, next_buses: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
        """
        Enriches the stop data with the information from other dataframes in the feed
        """
        joined_data = (next_buses
            .join(self.feed.trips.set_index("trip_id"), on="trip_id")
            .join(self.feed.stops.set_index("stop_id"), on="stop_id")
            .join(self.feed.routes.set_index("route_id"), on="route_id"))   

        return joined_data 


    def __filter_routes_by_stops(self, next_buses: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
        """
        Takes a dataframe of a set of bus arrivals and only shows the routes we are interested in 
        for the given stops (this is to eliminate routes that stop in more than one of our stops)
        """
        ids_to_delete = []

        for index, next_bus in next_buses.iterrows():
            stop_number = next_bus["stop_code"]
            route = next_bus["route_short_name"]
            routes_for_stop = self.routes_for_stops.get(int(stop_number), [])
            if len(routes_for_stop) > 0 and not route in routes_for_stop:
                # we should not show this entry. Note the ID
                ids_to_delete.append(index)
        
        next_buses.drop(index=ids_to_delete, inplace=True)
        return next_buses

    @staticmethod
    def __time_to_seconds(s: str) -> int:
        sx = s.split(":")
        if len(sx) != 3: 
            print("Malformed timestamp:", s)
            return 0
        return int(sx[0]) * 3600 + int(sx[1]) * 60 + int (sx[2])

    @staticmethod
    def __due_in_seconds(time_str: str) -> int:
        """
        Returns the number of seconds in the future that the time_str (format hh:mm:ss) is
        """
        now = datetime.datetime.now().strftime("%H:%M:%S")
        tnow = GTFSClient.__time_to_seconds(now)
        tstop = GTFSClient.__time_to_seconds(time_str)
        if tstop > tnow:
            return tstop - tnow
        else:
            # If the stop time is less than the current time, the stop is tomorrow
            return tstop + 86400 - tnow


    def __lookup_headsign_by_route(self, route_id: str, direction_id: int) -> str: 
        """
        Look up a destination string in Trips from the route and direction
        """
        trips = self.feed.trips
        destination = trips[(trips["route_id"] == route_id) & (trips["direction_id"] == direction_id)].head(1)["trip_headsign"].item()
        # For some reason destination sometimes isn't a string. Try to find out why
        if not destination.__class__ == str:
            sys.stderr.write("Destination not found for route " + str(route_id) + ", direction " + str(direction_id) + "\n")
            destination = "---- ?????? ----"
        
        return destination


    def __poll_gtfsr_deltas(self) -> tuple[dict, list, list]:
        try:
            # Poll GTFS-R API
            if self.gtfs_r_api_key != "":
                headers = {"x-api-key": self.gtfs_r_api_key}
                response = requests.get(url = self.gtfs_r_url, headers = headers, timeout=(2, 10))
                if response.status_code != 200:
                    print("GTFS-R sent non-OK response: {}\n{}".format(response.status_code, response.text))
                    return {}, [], []

                deltas_json = json.loads(response.text)
            else:
                deltas_json = json.load(open("example.json"))

            deltas = {}
            canceled_trips = set()
            added_stops = []

            # Pre-compute some data to use for added trips:
            relevant_service_ids = self.__current_service_ids()
            relevant_trips = self.feed.trips[self.feed.trips["service_id"].isin(relevant_service_ids)]
            relevant_route_ids = set(relevant_trips["route_id"])
            today = datetime.date.today().strftime("%Y%m%d")

            for e in deltas_json.get("entity", []):
                try:
                    trip_update = e.get("trip_update")
                    trip = trip_update.get("trip")
                    trip_id = trip.get("trip_id")
                    trip_action = trip.get("schedule_relationship")
                    if  trip_action == "SCHEDULED":
                        for u in e.get("trip_update", {}).get("stop_time_update", []): 
                            delay = u.get("arrival", u.get("departure", {})).get("delay", 0)
                            deltas_for_trip = (deltas.get(trip_id) or {})
                            deltas_for_trip[u.get("stop_id")] = delay
                            deltas[trip_id] = deltas_for_trip

                    elif trip_action == "ADDED":                    
                        start_date = trip.get("start_date")
                        start_time = trip.get("start_time")
                        route_id = trip.get("route_id")
                        direction_id = trip.get("direction_id")

                        # Check if the route is part of the routes we care about
                        if not route_id in relevant_route_ids:
                            continue

                        # And that it's for today
                        current_time = datetime.datetime.now().strftime("%H:%M:%S")
                        if start_date > today or start_time > current_time:
                            continue

                        # Look for the entry for any of the stops we want
                        wanted_stop_ids = self.__wanted_stop_ids()
                        for stop_time_update in e.get("trip_update").get("stop_time_update", []):
                            if stop_time_update.get("stop_id", "") in wanted_stop_ids:
                                arrival_time = int((stop_time_update.get("arrival", stop_time_update.get("departure", {})).get("time", 0)))
                                if arrival_time < int(time.time()):
                                    continue
                                new_arrival = ArrivalTime(
                                    stop_id = stop_time_update.get("stop_code"),
                                    route_id = self.feed.routes[self.feed.routes["route_id"] == route_id]["route_short_name"].item(), 
                                    destination = self.__lookup_headsign_by_route(route_id, direction_id), 
                                    due_in_seconds = arrival_time - int(time.time()),
                                    is_added = True
                                )
                                print("Added route:", new_arrival)
                                added_stops.append(new_arrival)

                    elif trip_action == "CANCELED":
                        canceled_trips.add(trip_id)
                    else:
                        print("Unsupported action:", trip_action)
                except Exception as x:
                    print("Error parsing GTFS-R entry:", str(e))
                    raise x
                
            return deltas, canceled_trips, added_stops
        except Exception as e:
            print("Polling for GTFS-R failed:", str(e))
            return {}, [], []


    def get_next_n_buses(self, num_entries: int) -> pd.core.frame.DataFrame:
        """
        Returns a dataframe with the information of the next N buses arriving at the requested stops.
        """
        service_ids = self.__current_service_ids()
        trip_ids = self.__trip_ids_for_service_ids(service_ids)
        next_buses = self.__next_n_buses(trip_ids, num_entries)
        joined_data = self.__join_data(next_buses)
        self.__filter_routes_by_stops(joined_data)
        return joined_data


    def refresh(self):
        """
        Create and enqueue the refreshed stop data
        """
        try:
            # Retrieve the GTFS-R deltas
            deltas, canceled_trips, added_stops = self.__poll_gtfsr_deltas()
            if len(deltas) > 0 or len(canceled_trips) > 0 or len(added_stops) > 0:
                # Only update deltas and canceled trips if the API returns data
                self.deltas = deltas
                self.canceled_trips = canceled_trips
                self.added_stops = added_stops

            arrivals = []
            # take more entries than we need in case there are cancellations
            buses = self.get_next_n_buses(15) 
            
            for index, bus in buses.iterrows():
                if not bus["trip_id"] in self.canceled_trips:
                    delta = self.deltas.get(bus["trip_id"], {}).get(bus["stop_id"], 0)
                    if delta != 0:
                        print("Delta for route {} stop {} is {}".format(bus["route_short_name"], bus["stop_id"], delta))

                    arrival = ArrivalTime(stop_id = bus["stop_code"], 
                                        route_id = bus["route_short_name"],
                                        destination = bus["trip_headsign"],
                                        due_in_seconds = GTFSClient.__due_in_seconds(bus["arrival_time"]) + delta,
                                        is_added = False
                    )
                    arrivals.append(arrival)

            if len(self.added_stops) > 0:
                # Append the added stops from GTFS-R and re-sort
                arrivals.extend(self.added_stops)
                arrivals.sort()

            # Select the first 5 of what remains
            arrivals = arrivals[0:5]

            if self._update_queue:
                self._update_queue.put(arrivals)

            gc.collect()
        except Exception as e:
            print("Exception in refresh: {}".format(str(e)))
