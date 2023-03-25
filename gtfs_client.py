from arrival_times import ArrivalTime
import datetime
import gtfs_kit as gk
import pandas as pd
import queue
import time
import threading
import traceback

class GTFSClient():
    def __init__(self, feed_name: str, stop_names: list[str], update_queue: queue.Queue, update_interval_seconds: int = 60):
        self.stop_names = stop_names
        self.feed = gk.read_feed(feed_name, dist_units='km')
        self.stop_ids = self.__wanted_stop_ids()

        # Schedule refresh       
        self.update_queue = update_queue
        if update_interval_seconds and update_queue: 
            self._refresh_thread = threading.Thread(target=lambda: every(self._update_interval_seconds, self.refresh))


    def __wanted_stop_ids(self) -> pd.core.frame.DataFrame:
        """
        Return a DataFrame with the ID and names of the chosen stop(s) as requested in station_names
        """
        stops = self.feed.stops[self.feed.stops["stop_name"].isin(self.stop_names)]
        if stops.empty: 
            raise Exception("Stops is empty!")
        return stops["stop_id"]


    def __service_ids_active_at(self, when: datetime) -> pd.core.frame.DataFrame:
        """
        Returns the service IDs active at a particular point in time
        """
        todays_date = when.strftime("%Y%m%d")
        todays_weekday = when.strftime("%A").lower()
        active_calendars = self.feed.calendar.query('start_date < @todays_date and end_date > @todays_date and {} == 1'.format(todays_weekday))
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
            raise Exception("There are no service IDs for today!")

        # Merge with the service IDs for tomorrow (in case the number of trips spills over to tomorrow)
        tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
        tomorrow_active = self.__service_ids_active_at(tomorrow)
        if tomorrow_active.empty:
            raise Exception("There are no service IDs for tomorrow!")

        active_calendars = pd.concat([now_active, tomorrow_active])
        if active_calendars.empty:
            raise Exception("The concatenation of today and tomorrow's calendars is empty. This should not happen.")
        
        return active_calendars["service_id"]


    def __trip_ids_for_service_ids(self, service_ids: pd.core.series.Series) -> pd.core.series.Series:
        """
        Returns a dataframe with the trip IDs for the given service IDs 
        """
        trips = self.feed.trips[self.feed.trips["service_id"].isin(service_ids)]
        if trips.empty:
            raise Exception("There are no active trips!")

        return trips["trip_id"]


    def __next_n_buses(self, 
                    trip_ids: pd.core.series.Series,
                    n: int) -> pd.core.frame.DataFrame:
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%m:%S")
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


    def get_next_n_buses(self, num_entries: int) -> pd.core.frame.DataFrame:
        """
        Returns a dataframe with the information of the next N buses arriving at the requested stops.
        """
        service_ids = self.__current_service_ids()
        trip_ids = self.__trip_ids_for_service_ids(service_ids)
        next_buses = self.__next_n_buses(trip_ids, num_entries)
        joined_data = self.__join_data(next_buses)
        return joined_data



    def refresh(self):
        """
        Create and enqueue the refreshed stop data
        """
        
        arrivals = []

        buses = self.get_next_n_buses(5)
        
        for index, bus in buses.iterrows():
            arrival = ArrivalTime(stop_id = bus["stop_id"], 
                                  route_id = bus["route_short_name"],
                                  destination= bus["route_long_name"].split(" - ")[1].strip(),
                                  due_in_seconds = 0
            )
            arrivals.append(arrival)

        if self.update_queue:
            self.update_queue.put(arrivals)
        return arrivals


def every(delay, task) -> None:
    """ Auxilliary function to schedule updates. 
        Taken from https://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds
    """
    next_time = time() + delay
    while True:
        time.sleep(max(0, next_time - time()))
        try:
            task()
        except Exception:
            traceback.print_exc()
            # in production code you might want to have this instead of course:
            # logger.exception("Problem while executing repetitive task.")
        # skip tasks if we are behind schedule:
        next_time += (time.time() - next_time) // delay * delay + delay


c = GTFSClient('google_transit_combined.zip', ['College Drive, stop 2410', 'Priory Walk, stop 1114'], None, None)

print(c.refresh())