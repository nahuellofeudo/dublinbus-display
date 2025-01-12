import yaml

class Config:
    def __init__(self):
        # Load the config file
        with open("config.yaml") as f:
            self.config = yaml.safe_load(f.read())

        # Preload some dictionaries to simplify lookups
        self.walk_time_by_stop = {}
        for s in self.config.get("stops", []):
            self.walk_time_by_stop[str(s["stop_id"])] = s["walk_time"]

    @property
    def gtfs_feed_url(self) -> str:
        return self.config.get("gtfs-feed-url")

    @property
    def gtfs_api_url(self) -> str:
        return self.config.get("gtfs-r-api-url")

    @property
    def gtfs_api_key(self) -> str:
        return self.config.get("gtfs-r-api_key")

    @property
    def update_interval_seconds(self) -> int:
        return self.config.get("update-interval-seconds")

    @property
    def font_file(self) -> str:
        return self.config.get("font-file")

    @property
    def stop_codes(self) -> list[str]:
        return [str(s["stop_id"]) for s in self.config.get("stops")]

    def minutes_to_stop(self, stop_id) -> int: 
        minutes = self.walk_time_by_stop.get(stop_id, 0)
        return minutes

    def routes_for_stops(self) -> map:
        result = {}

        for s in self.config.get("stops"):
            for r in s.get("routes", []):
                routes = (result.get(s.get("stop_id")) or [])
                routes.append(r)
                result[s.get("stop_id")] = routes
        return result