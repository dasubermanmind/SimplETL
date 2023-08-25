

from typing import Any, List
from bs4 import BeautifulSoup
import requests


class Scraper:

    def __init__(self, endpoint: str, div: str, file_name: str ,section: str = '', card_content: str = '', title: str = '', id: str = '', _class: str = '', links: List[Any] = [], 
                 h3: str = '', p: str = ''):
        self.endpoint = endpoint
        self.div = div
        self.file_name = file_name
        self.section= section
        self.card_content = card_content
        self.title = title
        self.id = id
        self._class = _class
        self.links = links
        self.h3 = h3
        self.p = p
        self.output = {}

    def get(self):

        session = requests.Session()
        session.get(self.endpoint)
        url = requests.get(self.endpoint)

        soup = BeautifulSoup(url.content, "html.parser")

        res = soup.find(id=self.id)

        elements = res.find_all(self.div, class_=self.card_content)

        elements2 = res.find_all(self.div, class_=self.section)
        elements3 = res.find_all(self.div, class_=self.card_content)
        elements4 = res.find_all(self.div, class_=self.title)

        for elems in elements:
            title_element = elems.find(self.div, class_=self._class)
            h3_element = elems.find(self.h3, class_=self._class)
            p_element = elems.find(self.p, class_=self._class)
            self.output = {
                f'{elems}': zip(dict(title_element, h3_element, p_element))
            }


        for elems in elements2:
            title_element = elems.find(self.div, class_=self._class)
            h3_element = elems.find(self.h3, class_=self._class)
            p_element = elems.find(self.p, class_=self._class)
            self.output = {
                f'{elems}': zip(dict(title_element, h3_element, p_element))
            }
            
        for elems in elements3:
            title_element = elems.find(self.div, class_=self._class)
            h3_element = elems.find(self.h3, class_=self._class)
            p_element = elems.find(self.p, class_=self._class)
            self.output = {
                f'{elems}': zip(dict(title_element, h3_element, p_element))
            }
            

        for elems in elements4:
            title_element = elems.find(self.div, class_=self._class)
            h3_element = elems.find(self.h3, class_=self._class)
            p_element = elems.find(self.p, class_=self._class)
            self.output = {
                f'{elems}': zip(dict(title_element, h3_element, p_element))
            }

        # Save locally
        with open(self.file_name, "w") as StreamWriter:
            StreamWriter.write(self.output)
            StreamWriter.write('\n')
            StreamWriter.close()


    def get_all_data(self):
        return self.output
        

        


