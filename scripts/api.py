import requests

class APIHandler:

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    def extract_api_data(self) -> dict:

        try:
            response = requests.get(self.endpoint,timeout=10)
            response.raise_for_status()
            print("Data extracted successfully from the API!")

            return response.json()

        except Exception as e:
            print(e)