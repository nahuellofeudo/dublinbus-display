import csv
from datetime import datetime, time, timedelta
import queue
import threading
from time import mktime
from io import TextIOWrapper
import json
import os
import traceback
import urllib.request
import zipfile
from arrival_times import ArrivalTime

# Constants and configuration
GTFS_BASE_DATA_URL = "https://www.transportforireland.ie/transitData/google_transit_combined.zip"
GTFS_R_URL = "https://api.nationaltransport.ie/gtfsr/v1?format=json"
API_KEY = '470fcdd00bfe45c188fb236757d2df4f'
BASE_DATA_MINSIZE = 20000000  # The zipped base data should be over 20-ish megabytes

class GTFSClient:
    def __init__(self, wanted_stops : list[str], update_queue: queue.Queue, update_interval_seconds: int = 60):
        self.wanted_stops = wanted_stops
        self._update_queue = update_queue
        self._update_interval_seconds = update_interval_seconds

        # Check that the base data exists, and download it if it doesn't 
        base_data_file_name = GTFS_BASE_DATA_URL.split('/')[-1]
        if not os.path.isfile(base_data_file_name):
            try:
                urllib.request.urlretrieve(GTFS_BASE_DATA_URL, base_data_file_name)
                if not os.path.isfile(base_data_file_name):
                    raise Exception("The file %s was not downloaded.".format(base_data_file_name))
                if os.path.getsize(base_data_file_name) < BASE_DATA_MINSIZE:
                    raise Exception("The base data file {} was too small.".format(base_data_file_name))
            except Exception as e:
                print("Error downloading base data: {}".format(str(e)))
                raise e
        
        # Preload the entities from the base data
        with zipfile.ZipFile(base_data_file_name) as zipped_base_data:
            # Load stops and select the stop IDs we are interested in
            print ('Loading stops...')
            stops = self.loadfrom(zipped_base_data, "stops.txt", 
                                  lambda s: s['stop_name'] in wanted_stops)
            self.selected_stop_ids = set([s['stop_id'] for s in stops])
            self.stops_by_stop_id = {}
            for stop in stops:
                self.stops_by_stop_id[stop['stop_id']] = stop

            # Load the stop times for the selected stops
            print ('Loading stop times...')
            self.stop_time_by_stop_and_trip_id = {}
            self.selected_trip_ids = set()
            stop_times = self.loadfrom(zipped_base_data, "stop_times.txt",
                                       lambda st: st['stop_id'] in self.selected_stop_ids )
            for st in stop_times:
                self.stop_time_by_stop_and_trip_id[(st['stop_id'], st['trip_id'])] = st
                self.selected_trip_ids.add(st['trip_id'])

            # Load the trips that include the selected stops
            print ('Loading trips...')
            self.trip_by_trip_id = {}
            self.selected_route_ids = set()
            trips = self.loadfrom(zipped_base_data, "trips.txt", 
                                  lambda t: t['trip_id'] in self.selected_trip_ids)
            for t in trips:
                self.trip_by_trip_id[t['trip_id']] = t
                self.selected_route_ids.add(t['route_id'])

            # Load the names of the routes for the selected trips
            routes = self.loadfrom(zipped_base_data, 'routes.txt', 
                                   lambda r: r['route_id'] in self.selected_route_ids)   
            self.route_name_by_route_id = {}
            for r in routes:
                self.route_name_by_route_id[r['route_id']] = r['route_short_name']

        # Schedule refresh       
        if update_interval_seconds: 
            self._refresh_thread = threading.Thread(target=lambda: every(self._update_interval_seconds, self.refresh))


    def loadfrom(self, zipfile: zipfile.ZipFile, name: str, filter: callable = None) -> map:
        """
        Load a CSV file from the zip
        """
        with zipfile.open(name, "r") as datafile:
            if not datafile:
                raise Exception('File %s is not in the zipped data'.format(name))
            if filter:
                result = []
                for r in csv.DictReader(TextIOWrapper(datafile, "utf-8-sig")):
                    if filter(r):
                        result.append(r)
            else:
                result = [r for r in csv.DictReader(TextIOWrapper(datafile, "utf-8-sig"))]
        return result


    def update_schedule_from(self, gtfsr_json: str) -> list:
        """
        Creates a structure with the routes and arrival times from the 
        preloaded information, plus the gtfsr data received from the API
        """
        # Parse JSON
        gtfsr_data = json.loads(gtfsr_json)
        entities = gtfsr_data['Entity']

        arrivals = []

        for e in entities:
            # Skip non-updates and invalid entries
            if (e.get('IsDeleted')
                or not e.get('TripUpdate') 
                or not e['TripUpdate'].get('Trip')
                or not e['TripUpdate']['Trip'].get('TripId') in self.selected_trip_ids): 
                continue

            # e contains an update for a trip we are interested in.
            stop_times = e['TripUpdate'].get('StopTimeUpdate')
            if not stop_times:
                print('A TripUpdate entry does not have StopTimeUpdate:')
                print(e)
                continue

            for st in stop_times:
                # Skip the stops we are not interested in
                if not st.get('StopId') in self.selected_stop_ids: 
                    continue

                # We have a stop time for one of our stops. Collect all info
                trip_id = e['TripUpdate']['Trip']['TripId']
                trip = self.trip_by_trip_id[trip_id]
                trip_destination = trip['trip_headsign']
                if len(trip_destination.split(' - ')) > 1:
                    trip_destination = trip_destination.split(' - ')[1]
                route_id = self.trip_by_trip_id[trip_id]['route_id']
                route_name = self.route_name_by_route_id[route_id]
                stop_name = self.stops_by_stop_id[st['StopId']]['stop_name']
                stop_time = self.calculate_delta(
                    self.stop_time_by_stop_and_trip_id[(st['StopId'], trip_id)]['arrival_time'],
                    st['Arrival'].get('Delay') or 0
                )
                current_timestamp = (mktime(datetime.now().timetuple()))
                due_in_seconds = stop_time - current_timestamp
                
                arrival_time = ArrivalTime(stop_name, route_name, trip_destination, due_in_seconds)
                arrivals.append(arrival_time)
        arrivals = sorted(arrivals)
        return arrivals

    def refresh(self) -> None:
        """ Poll for new and updated information. Queue it for display update. """
        # Retrieve the updated json
        url_opener = urllib.request.URLopener()
        url_opener.addheader('x-api-key', API_KEY)
        response = url_opener.open(GTFS_R_URL)
        gtfs_r_json = response.file.read()
        arrivals = gtfs.update_schedule_from(gtfs_r_json)
        self._update_queue.put(arrivals)

    def calculate_delta(self, stop_time: str, delta: int) -> datetime:
        """
        Returns a unix timestamp of 
        """
        stop_time_parts = list(map(lambda n: int(n), stop_time.split(':')))
        initial = datetime.combine(datetime.now(), 
                                   time(stop_time_parts[0], stop_time_parts[1], stop_time_parts[2]))
        adjusted = initial + timedelta(seconds = delta)
        return int(mktime(adjusted.timetuple()))


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


if __name__ == "__main__":
    gtfs = GTFSClient([
        "Priory Walk, stop 1114",
        "College Drive, stop 2410",
        "Kimmage Road Lower, stop 2438",
        "Brookfield, stop 2437"
        ], queue.Queue(), None)

    if True:
        o = urllib.request.URLopener()
        o.addheader('x-api-key', API_KEY)
        r = o.open(GTFS_R_URL)
        if r.code != 200:
            print(r.file.read())
            exit(1)
        gtfs_r_json = r.file.read()
    else:    
        gtfs_r_json = open('example.json').read()

    arrivals = gtfs.update_schedule_from(gtfs_r_json)
    print(gtfs)


