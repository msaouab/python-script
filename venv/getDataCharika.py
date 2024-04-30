from concurrent.futures import ThreadPoolExecutor
from functools import cache
import json
import re
import time
from requests_futures.sessions import FuturesSession
from bs4 import BeautifulSoup
from requests import session
import requests
import concurrent.futures

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
data_dict = {}
cache = {}
session = FuturesSession(max_workers=16)
start_time = time.time()
index = 1

def extract_name(soup):
	name = soup.find('h1', class_='nom society-name').text.strip()
	return 'name', name

def extract_rating(soup):
	global_rating_div = soup.find('div', class_='global-rating col-md-12 nopadding')
	if global_rating_div:
		rating = global_rating_div.find('a', class_='showHiddenInfos')
		if rating:
			rating = rating.text.strip()
		return 'rating', rating
	return 'rating', None

def extract_address(soup):
	address_tag = soup.find('div', class_='row ligne-tfmw col-md-6')
	address = address_tag.text.strip().replace("Adresse", "").strip() if address_tag else None
	return 'address', address

def extract_phone(soup):
	tel_table = soup.find('table', class_='table nomargin')
	if tel_table:
		phone_ele = tel_table.find_all('span', class_='marketingInfoTelFax')
		phone = [tel.text.strip() for tel in phone_ele]
		return 'phone', phone
	return 'phone', None

def extract_fax(soup):
	fax_elements = soup.find_all('span', class_='mrg-fiche2')
	fax = [fax_element.find_next('span', class_='marketingInfoTelFax').text.strip() for fax_element in fax_elements]
	return 'fax', fax

def extract_email(soup):
	email_elements = soup.find_all('span', class_='mrg-fiche3')
	email_links = [email_element.find_next('a')['href'] for email_element in email_elements]
	emails = [link.split(':')[-1] for link in email_links]
	return 'mail', emails

def extract_website(soup):
	website_elements = soup.find_all('span', class_='mrg-fiche4')
	website = [website_element.find_next('a')['href'] for website_element in website_elements]
	return 'website', website

def extract_status(soup):
	status_tag = soup.find('i', class_='folder-openicon- icon-fw')
	status = status_tag.find_parent('td').find_next_sibling('td').text.strip() if status_tag else None
	return 'status', status

def extract_capital(soup):
	capital_tag = soup.find('i', class_='moneyicon- icon-fw')
	capital = capital_tag.find_parent('td').find_next_sibling('td').text.strip() if capital_tag else None
	return 'capital', capital

def extract_latitude(soup):
	latitude = soup.find('input', class_='latitude')['value']
	return 'latitude', latitude

def extract_longitude(soup):
	longitude = soup.find('input', class_='longitude')['value']
	return 'longitude', longitude

def extract_activity(soup):
	activity_tag = soup.find('span', title=True)
	activity = activity_tag.find_next('h2').text.strip() if activity_tag else None
	return 'activity', activity

def extract_data_parallel(link):
	global index
	print("Fetching data for", index, "companies...")
	index += 1
	if link in cache:
		html = cache[link]
	else:
		while True:
			try:
				response = session.get(link).result()
				html = response.text
				cache[link] = html
				break
			except requests.exceptions.RequestException as e:
				print("An error occurred while fetching data from", link)
				print("Error:", e)
				print("Retrying...")
				time.sleep(10)
				continue
	soup = BeautifulSoup(html, 'html.parser')
	futures = []
	with ThreadPoolExecutor(max_workers=16) as executor:
		futures.append(executor.submit(extract_name, soup))
		futures.append(executor.submit(extract_rating, soup))
		futures.append(executor.submit(extract_address, soup))
		futures.append(executor.submit(extract_phone, soup))
		futures.append(executor.submit(extract_fax, soup))
		futures.append(executor.submit(extract_email, soup))
		futures.append(executor.submit(extract_website, soup))
		futures.append(executor.submit(extract_status, soup))
		futures.append(executor.submit(extract_capital, soup))
		futures.append(executor.submit(extract_latitude, soup))
		futures.append(executor.submit(extract_longitude, soup))
		futures.append(executor.submit(extract_activity, soup))

	data = {}
	for future in concurrent.futures.as_completed(futures):
		key, value = future.result()
		data[key] = value

	return data

def process_data(data_dict, outfile, num_workers):
	data = []
	with ThreadPoolExecutor(max_workers=num_workers) as executor:
		company_links = [f'{url}{link}' for link in data_dict]
		print("Fetching data for", len(company_links), "companies...")
		futures = {executor.submit(extract_data_parallel, link): link for link in company_links}

		for future in concurrent.futures.as_completed(futures):
			data.append(future.result())

		filename = f"./data/{outfile}_data.json"
		with open(filename, 'w', encoding='utf-8') as file:
			json.dump(data, file, ensure_ascii=False, indent=4)

def readJsonFile(filename):
	infile = f"./data/{filename}_links.json"
	try:
		with open(infile, 'r') as file:
			data_dict = json.load(file)
			print("Read Json file successfully.", len(data_dict))
	except FileNotFoundError:
		print(f"File {infile} not found.")
		exit()
	return data_dict

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
	start_time = time.time()
	data_dict = readJsonFile(filename)
	num_workers = 16
	process_data(data_dict, filename, num_workers)
	time_taken = time.time() - start_time
	print(f"Time taken: {time_taken / 60} minutes")

if __name__ == '__main__':
	main()
