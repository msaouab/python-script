import json
from multiprocessing import Pool
import os
import re
import sys
import time
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By

url = "https://www.charika.ma/"
sActivite = {
    "J": "Activités financières",
    "A": "Agriculture, chasse, sylviculture",
    "F": "Bâtiment et travaux publics",
    "G": "Commerce; réparations automobile et d'articles domestiques",
    "M": "Education",
    "H": "Hôtels et Restaurants",
    "K": "Immobiliers, location et services aux entreprises",
    "C": "Industries extractives",
    "D": "Industries manufacturières",
    "B": "Pêche, aquaculture",
    "E": "Production et distribution d'électricité, de gaz et d'eau",
    "N": "Santé et action sociale",
    "O": "Services collectifs, sociaux et personnels",
    "P": "Services domestiques",
    "I": "Transports et Communications"
}

session = requests.Session()

# def fetch_links_for_page(cookies, page_num):
#     page_link = url + f"societes-{page_num}"
#     print(f"Fetching links for page {page_num}...")
#     response = session.get(page_link, cookies=cookies)
#     soup = BeautifulSoup(response.text, 'html.parser')
#     anchor_tags = soup.select('.panel-body .text-soc h5 a')
#     links_set = {anchor['href'] for anchor in anchor_tags if 'href' in anchor.attrs}
#     return links_set

def fetch_links_for_page(args):
    page_num, cookies = args
    page_link = url + f"societes-{page_num}"
    print(f"Fetching links for page {page_num}...")
    retries = 3
    while retries > 0:
        try:
            response = session.get(page_link, cookies=cookies, timeout=10)  # Adjust timeout as needed
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            anchor_tags = soup.select('.panel-body .text-soc h5 a')
            links_set = {anchor['href'] for anchor in anchor_tags if 'href' in anchor.attrs}
            return links_set
        except requests.RequestException as e:
            print(f"Error fetching page {page_num}: {e}")
            retries -= 1
            if retries == 0:
                print(f"Failed to fetch page {page_num} after retries.")
                return set()

def readConsoleInput():
    for key, value in sActivite.items():
        print(f"{key} : {value}")
    userInput = input("Enter the activity code: ")
    if userInput not in sActivite:
        print("Invalid activity code.")
        exit()
    return userInput

def main():
    activity = readConsoleInput()
    filename = '_'.join(re.split(r'\W+', sActivite[activity]))
    print(f"Fetching data for {sActivite[activity]}, filename: {filename}.json")
    json_filename = f"./data/{filename}_links.json"
    start_time = time.time()
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    driver.find_element(By.ID, 'societe').click()
    cookies_dict = driver.get_cookie('JSESSIONID')
    cookies = {cookies_dict['name']: cookies_dict['value']}
    try:
        selected_index = list(sActivite.keys()).index(activity)
        print(f"{activity}: {sActivite[activity]}, index: {selected_index}")
        buttons = driver.find_elements(By.CSS_SELECTOR, "button.btn.dropdown-toggle.btn-default.bs-placeholder")
        buttons[2].click()
        options = driver.find_elements(By.CSS_SELECTOR, "ul.dropdown-menu.inner li")
        options[selected_index + 1].click()
        search_button = driver.find_element(By.CSS_SELECTOR, "button.btn.btn-color")
        search_button.click()
        pagination = driver.find_element(By.CLASS_NAME, 'pagination')
        last_page_link = pagination.find_elements(By.TAG_NAME, 'a')[-1]
        last_page_url = last_page_link.get_attribute('href')
        last_page = int(last_page_url.split('-')[-1])
        all_links = set()
        # for page_num in range(1, last_page + 1):
        #     links_set = fetch_links_for_page(cookies, page_num)
        #     all_links.update(links_set)

        directory = './data/'
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Multiprocessing pool
        with Pool() as pool:
            pages = range(1, last_page + 1)
            args = [(page, cookies) for page in pages]
            results = pool.map(fetch_links_for_page, args)
            for links_set in results:
                all_links.update(links_set)

            with open(json_filename, 'w', encoding='utf-8') as json_file:
                json.dump(list(all_links), json_file, ensure_ascii=False, indent=2)
        activity_time = time.time() - start_time
        print(f"Time taken for {sActivite[activity]}: {activity_time / 60} minutes")
    except KeyboardInterrupt:
        print("KeyboardInterrupt: Exiting...")
        sys.exit(0)
    finally:
        driver.quit()

if __name__ == '__main__':
    main()