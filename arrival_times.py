import datetime

class ArrivalTime():
    """ Represents the arrival times of buses at one of the configured stops """

    def __init__(self, stop_id: str, route_id: str, destination: str, due_in_seconds: int) -> None:
        self.stop_id = stop_id
        self.route_id = route_id
        self.destination = destination
        self.due_in_seconds = due_in_seconds

    @property
    def due_in_minutes(self) -> int:
        return int(self.due_in_seconds / 60)

    def isDue(self) ->  bool:
        return self.due_in_minutes < 1

    def due_in_str(self) -> str:
        if self.due_in_minutes < 99:
            return str(self.due_in_minutes) + "min"
        else:
            due_in = datetime.datetime.now() + datetime.timedelta(0, self.due_in_seconds)
            return due_in.strftime("%H:%M")

    def __lt__(self, other) -> int:
        return self.due_in_seconds < other.due_in_seconds