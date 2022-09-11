import iso8601
import queue
import time
import traceback
import threading
import xml.etree.ElementTree as ET
from zeep import Client
from arrival_times import ArrivalTime

# Path to StopData elements within the XML
STOPDATA_PATH = ('{http://schemas.xmlsoap.org/soap/envelope/}Body/' + 
                '{http://dublinbus.ie/}GetRealTimeStopDataResponse/' + 
                '{http://dublinbus.ie/}GetRealTimeStopDataResult/' + 
                '{urn:schemas-microsoft-com:xml-diffgram-v1}diffgram/' + 
                'DocumentElement')


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


def parse_seconds(time_expr: str) -> int:
    """ Parses an XML timestamp and returns the seconds since the epoch 
        E.g. 2022-09-10T20:33:30.387+01:00
    """
    return int(iso8601.parse_date(time_expr).timestamp())


class DublinBusSoapClient:
    """ Code to pull updates of the requested stops """

    def __init__(self, stops: list[str], update_queue: queue.Queue, update_interval_seconds: int = 60) -> None :
        
        # Store parameters
        self._stops = stops
        self._update_queue = update_queue
        self._update_interval_seconds = update_interval_seconds

        # Create SOAP Client
        self._client = Client('http://rtpi.dublinbus.ie/DublinBusRTPIService.asmx?WSDL')

        # Schedule refresh        
        self._refresh_thread = threading.Thread(target=lambda: every(self._update_interval_seconds, self.refresh))


    def start(self) -> None:
        """ Start the refresh thread """
        self._refresh_thread.start()
        self.refresh()

    def refresh(self) -> None:
        arrivals = []
        for stop in self._stops:
            with self._client.settings(raw_response=True):
                response = self._client.service.GetRealTimeStopData(stopId=stop, forceRefresh=True)
                if response.ok:
                    tree = ET.fromstring(response.text)
                    stopdata_elements = tree.find(STOPDATA_PATH)
                    for stopdata in (stopdata_elements or []): 
                        route = stopdata.find('MonitoredVehicleJourney_PublishedLineName').text
                        destination = stopdata.find('MonitoredVehicleJourney_DestinationName').text
                        due_in_seconds = (parse_seconds(stopdata.find('MonitoredCall_ExpectedArrivalTime').text) 
                                        - parse_seconds(stopdata.find('Timestamp').text))
                        arrival_time = ArrivalTime(stop, route, destination, due_in_seconds)
                        arrivals.append(arrival_time)
        arrivals = sorted(arrivals)
        self._update_queue.put(arrivals)
            