from arrival_times import ArrivalTime
import datetime
import gtfs_kit as gk
import json
import os
import pandas as pd
import queue
import re
import refresh_feed
import requests
import tempfile
import time
import threading
import traceback
import shutil
import zipfile

class GTFSClient():
    GTFS_URL = "https://api.nationaltransport.ie/gtfsr/v2/gtfsr?format=json"
    API_KEY = open("api-key.txt").read().strip()

    def __init__(self, feed_url: str, stop_codes: list[str], update_queue: queue.Queue, update_interval_seconds: int = 60):
        self.stop_codes = stop_codes
        feed_name = feed_url.split('/')[-1]

        # Make sure that the feed file is up to date
        try:
            last_mtime = os.stat(feed_name).st_mtime
        except:
            last_mtime = 0

        refreshed, new_mtime = refresh_feed.update_local_file_from_url_v1(last_mtime, feed_name, feed_url)
        if refreshed:
            print("The feed file was refreshed.")
        else:
            print("The feed file was up to date")

        # Load the feed
        self.feed = self._read_feed(feed_name, dist_units='km', stop_codes = stop_codes)
        self.stop_ids = self.__wanted_stop_ids()
        self.deltas = {}
        self.canceled_trips = set()

        # Schedule refresh       
        self._update_queue = update_queue
        if update_interval_seconds and update_queue: 
            self._update_interval_seconds = update_interval_seconds
            self._refresh_thread = threading.Thread(target=lambda: every(update_interval_seconds, self.refresh))

    def _read_feed(self, path: gk.Path, dist_units: str, stop_codes: list[str]) -> gk.Feed:
        """
        NOTE: This helper method was extracted from gtfs_kit.feed to modify it
        to only load the stop_times for the stops we are interested in,
        because loading the entire feed would use more memory than the Raspberry Pi Zero W has.

        This version also reads CSV data straight from the zip file to avoid
        wearing out the Pi's SD card.
        """
        FILES_TO_LOAD = [
            # List of feed files to load. stop_times.txt is loaded separately.
            'shapes.txt',
            'trips.txt',
            'routes.txt',
            'calendar.txt',
            'calendar_dates.txt',
            'stops.txt',
            'agency.txt'
        ]

        path = gk.Path(path)
        if not path.exists():
            raise ValueError(f"Path {path} does not exist")

        feed_dict = {table: None for table in gk.cs.GTFS_REF["table"]}
        with zipfile.ZipFile(path) as z:
            for filename in FILES_TO_LOAD:
                table = filename.split(".")[0]
                # read the file
                with z.open(filename) as f:
                    df = pd.read_csv(f, dtype=gk.cs.DTYPE, encoding="utf-8-sig")
                    if not df.empty:
                        feed_dict[table] = gk.cn.clean_column_names(df)

            # Finally, load stop_times.txt
            # Obtain the list of IDs of the desired stops. This is similar to what __wanted_stop_ids() does, 
            # but without a dependency on a fully formed feed object
            wanted_stop_ids = feed_dict.get("stops")[feed_dict.get("stops")["stop_code"].isin(stop_codes)]["stop_id"]
            with z.open("stop_times.txt") as f:
                iter_csv = pd.read_csv(f, iterator=True, chunksize=1000, dtype=gk.cs.DTYPE, encoding="utf-8-sig")
                df = pd.concat([chunk[chunk["stop_id"].isin(wanted_stop_ids)] for chunk in iter_csv])

            if not df.empty:
                feed_dict["stop_times"] = gk.cn.clean_column_names(df)

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


    def __current_service_ids(self) -> pd.core.series.Series:
        """
        Filter the calendar entries to find all service ids that apply for today.
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
        
        return active_calendars["service_id"]


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

    def __time_to_seconds(self, s: str) -> int:
        sx = s.split(":")
        if len(sx) != 3: 
            print("Malformed timestamp:", s)
            return 0
        return int(sx[0]) * 3600 + int(sx[1]) * 60 + int (sx[2])

    def __due_in_seconds(self, time_str: str) -> int:
        """
        Returns the number of seconds in the future that the time_str (format hh:mm:ss) is
        """
        now = datetime.datetime.now().strftime("%H:%M:%S")
        tnow = self.__time_to_seconds(now)
        tstop = self.__time_to_seconds(time_str)
        if tstop > tnow:
            return tstop - tnow
        else:
            # If the stop time is less than the current time, the stop is tomorrow
            return tstop + 86400 - tnow


    def __poll_gtfsr_deltas(self) -> list[map, set]:

        # Poll GTFS-R API
        if GTFSClient.API_KEY != "":
            headers = {"x-api-key": GTFSClient.API_KEY}
            response = requests.get(url = GTFSClient.GTFS_URL, headers = headers)
            if response.status_code != 200:
                print("GTFS-R sent non-OK response: {}\n{}".format(response.status_code, response.text))
                return (None, None)

            deltas_json = json.loads(response.text)
        else:
            deltas_json = json.load(open("example.json"))

        deltas = {}
        canceled_trips = set()

        for e in deltas_json.get("entity"):
            is_deleted = e.get("is_deleted") or False
            try:
                trip_id = e.get("trip_update").get("trip").get("trip_id")
                trip_action = e.get("trip_update").get("trip").get("schedule_relationship")
                if  trip_action == "SCHEDULED":
                    for u in e.get("trip_update").get("stop_time_update"): 
                        delay = u.get("arrival", u.get("departure", {})).get("delay", 0)
                        deltas_for_trip = (deltas.get(trip_id) or {})
                        deltas_for_trip[u.get("stop_id")] = delay
                        deltas[trip_id] = deltas_for_trip

                elif trip_action == "ADDED":
                    # TODO: Add support for added trips
                    pass
                else:
                    canceled_trips.add(trip_id)
            except Exception as x:
                print("Error parsing GTFS-R entry:", str(e))
                raise(x)
            
        return deltas, canceled_trips


    def get_next_n_buses(self, num_entries: int) -> pd.core.frame.DataFrame:
        """
        Returns a dataframe with the information of the next N buses arriving at the requested stops.
        """
        service_ids = self.__current_service_ids()
        trip_ids = self.__trip_ids_for_service_ids(service_ids)
        next_buses = self.__next_n_buses(trip_ids, num_entries)
        joined_data = self.__join_data(next_buses)
        return joined_data


    def start(self) -> None:
        """ Start the refresh thread """
        self._refresh_thread.start()
        self.refresh()


    def refresh(self):
        """
        Create and enqueue the refreshed stop data
        """
        # Retrieve the GTFS-R deltas
        deltas, canceled_trips = self.__poll_gtfsr_deltas()
        if deltas:
            # Only update deltas and canceled trips if the API returns data
            self.deltas = deltas
            self.canceled_trips = canceled_trips

        # 
        arrivals = []
        # take more entries than we need in case there are cancelations 
        buses = self.get_next_n_buses(10) 
        
        for index, bus in buses.iterrows():
            if not bus["trip_id"] in self.canceled_trips:
                delta = self.deltas.get(bus["trip_id"], {}).get(bus["stop_id"], 0)
                if delta != 0:
                    print("Delta for route {} stop {} is {}".format(bus["route_short_name"], bus["stop_id"], delta))

                arrival = ArrivalTime(stop_id = bus["stop_id"], 
                                    route_id = bus["route_short_name"],
                                    # So apparently the GTFS feed uses non-ascii dashes because OF COURSE THEY DO
                                    destination = re.split(r"–|-", bus["route_long_name"])[-1].strip(),
                                    due_in_seconds = self.__due_in_seconds(bus["arrival_time"]) + delta
                )
                arrivals.append(arrival)

        # Select the first 5 of what remains
        arrivals = arrivals[0:5]

        if self._update_queue:
            self._update_queue.put(arrivals)
        return arrivals
    

def every(delay, task) -> None:
    """ Auxilliary function to schedule updates. 
        Taken from https://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds
    """
    next_time = time.time() + delay
    while True:
        time.sleep(max(0, next_time - time.time()))
        try:
            task()
        except Exception:
            traceback.print_exc()
            # in production code you might want to have this instead of course:
            # logger.exception("Problem while executing repetitive task.")
        # skip tasks if we are behind schedule:
        next_time += (time.time() - next_time) // delay * delay + delay

if __name__ == "__main__":
    c = GTFSClient('https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip', 
                   ['2410', '1114'], None, None)
    print(c.refresh())
