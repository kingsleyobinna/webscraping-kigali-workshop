import time
from datetime import date
from urllib.robotparser import RobotFileParser
import json

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pydantic import BaseModel


def safe_get(s: requests.sessions.Session, robots: RobotFileParser, link: str) -> requests.models.Response:
    """Wrapper for a request session get call that respects the robots.txt file
    Parameters:
        s (requests.sessions.Session): Requests session with User-Agent properly set
        robots (RobotFileParser): Initialized robots.txt parsed object for the specific website
        link (str): link that you want to retrive respecting the robots.txt file
    
    Returns:
        Response from session get call or None if the link is forbidden by robots.txt
    """
    if robots.can_fetch(s.headers.get("User-Agent"), link):
        response = s.get(link)
    else:
        response = None
    return response


def scrape_category(
    link: str, 
    category: str,
    Item: BaseModel,
    s: requests.session(),
    delay: float = 1) -> list:
    """Function to scrape a category following pagination.
    Parameters:
        link (str): starting link for a category
        category (str): category name
        Item (BaseModel): class of the data object for the specific source
        s (requests.Session()): Requests session with User-Agent properly set
        delay (float): delay in seconds between calls to prevent overloading the source

    Returns:
        list of product with all information
    """
    time.sleep(delay)
    page = s.get(link)
    page = BeautifulSoup(page.text, 'html.parser')
    links = [item.find("a").get("href") for item in page.find_all("h3", {"class": "product-title"})]
    results = []
    for l in links[:3]:
        time.sleep(delay)
        product = s.get(l)
        product = BeautifulSoup(product.text, 'html.parser')
        parsed_product = {}
        parsed_product["link"] = l
        parsed_product["name"] = product.find("h1", {"class": "product_title"}).get_text()
        if product.find("form", {"class": "variations_form"}):
            data_json = json.loads(product.find("form", {"class": "variations_form"}).get("data-product_variations"))
            for item in data_json:
                parsed_product["size"] = [v for k,v in item.get("attributes").items() if k.startswith("attri")][0]  # It need improvement
                parsed_product["price"] = item.get("display_price")
                
                results.append(Item(**parsed_product))
        else:
            data_json = json.loads(
                product.find(
                    "script",
                    {"type": "application/ld+json"},
                    class_=lambda x: x!= "yoast-schema-graph"
                ).get_text()
            )
            parsed_product["price"] = data_json.get("offers")[0].get("price")
            parsed_product["description"] = data_json.get("description")
            results.append(Item(**parsed_product))

    # Follow pagination if exists
    try:
        next_page = page.find("ul", {"class": "page-numbers"}).find("a", {"class": "next"})
        if next_page is not None:
            next_page = next_page.get("href")
            next_results = scrape_category(link=next_page, category=category, Item=Item, s=s, delay=delay)
            results.extend(next_results)
    except AttributeError:
        pass
            
    return results


class Product(BaseModel):
    link: str
    source: str
    category: str = None
    subcategory: str = None
    subsubcategory: str = None
    name: str = None
    brand: str = None
    size: str = None
    uid: str = None
    price: float
    regular_price: float = None
    currency: str
    in_stock: str = None
    description: str = None
    date: str = date.today().strftime("%Y-%m-%d")

class Themarket(Product):
    source: str = "The Market Food Shop"
    currency: str = "NGN"
    
    
root_url = "https://www.themarketfoodshop.com/"

# Setup session and user-agent
headers = {  # Need to be replaced with your details
    'User-Agent': 'Webscraping Capacity Building 1.0',
    'From': 'koibegbulam@nigerianstat.gov.ng'  
}

s = requests.Session()
s.headers.update(headers)

# Parse robots.txt
robots_tfms = RobotFileParser(root_url + "robots.txt")
robots_tfms.read()
delay = robots_tfms.crawl_delay(s.headers.get("User-Agent"))

# Get category links
homepage = s.get(root_url)
page = BeautifulSoup(homepage.text, 'html.parser')
div_list = page.find_all("div", {"class": "sub-menu-dropdown"})
link_list = []
for div in div_list:
    for l in div.find_all("a"):
        new_link = {}
        new_link["name"] = l.get_text()
        new_link["link"] = l.get("href")
        link_list.append(new_link)

# Scrape all products
data_list = []

for cat in link_list:
    data_list.extend(
        scrape_category(
            link = cat.get("link"), 
            category = cat.get("name"),
            Item = Themarket,
            s = s,
            delay = delay
        )
    )

data_df = pd.DataFrame([prod.dict(exclude_none=True) for prod in data_list])
data_df.to_csv("themarketfoodshop_{}.csv".format(date.today().strftime("%Y-%m-%d")), index=False)

