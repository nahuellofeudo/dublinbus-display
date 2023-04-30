import yaml

class Config:
    def __init__(self):
        # Load the config file
        with open("config.yaml") as f:
            self.__config = yaml.safe_load(f.read())

        # Pre-load some dictionaries to simplify lookups
        self.__walk_time_by_stop = {}
        for s in self.__config.get("stops", []):
            self.__walk_time_by_stop[s["stop_id"]] = s["walk_time"]

    @property
    def gtfs_feed_url(self) -> str:
        return self.__config.get("gtfs-feed-url")

    @property
    def gtfs_api_url(self) -> str:
        return self.__config.get("gtfs-r-api-url")

    @property
    def gtfs_api_key(self) -> str:
        return self.__config.get("gtfs-r-api_key")

    @property
    def update_interval_seconds(self) -> int:
        return self.__config.get("update-interval-seconds")

    @property
    def stop_codes(self) -> list[str]:
        return [str(s["stop_id"]) for s in self.__config.get("stops")]

    def minutes_to_stop(self, stop_id) -> int: 
        return self.__walk_time_by_stop.get(stop_id, 0)
