

import requests


class Scraper:

    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def get(self):
        session = requests.Session()
        session.get(self.endpoint)
        url = requests.get(self.endpoint)

        soup = BeautifulSoup(url.content, "html.parser")
        print(soup)


