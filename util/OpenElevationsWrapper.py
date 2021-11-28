import requests

class OpenElevationsApi:
    URI = "https://api.open-elevation.com/api/v1/lookup"
    LOCATIONS = list()

    def withLoc(self, lat, long) -> None:
        self.LOCATIONS.append({
            "latitude": lat,
            "longitude": long
        })

    def withMultipleLoc(self, locations: list) -> None:
        self.LOCATIONS = locations

    def constructBody(self):
        return {
            "locations": self.LOCATIONS
        }

    def get(self):
        raise NotImplementedError("OpenElevationsApi.get() is not implemented")
        
    def post(self):
        return requests.post(self.URI, json=self.constructBody())