from functools import cache
import json
from multiprocessing import Pool
import re
import time
from requests_futures.sessions import FuturesSession
from bs4 import BeautifulSoup
from requests import session
import requests

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

def extract_data(link):
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

	company_data = {}
	name = soup.find('h1', class_='nom society-name').text.strip()
	company_data['name'] = name
	global_rating_div = soup.find('div', class_='global-rating col-md-12 nopadding')
	if global_rating_div:
		rating = global_rating_div.find('a', class_='showHiddenInfos')
		if rating:
			rating = rating.text.strip()
		company_data['rating'] = rating
	address_tag = soup.find('div', class_='row ligne-tfmw col-md-6')
	address = address_tag.text.strip().replace("Adresse", "").strip() if address_tag else None
	company_data['address'] = address
	if address:
		location = address.split(' - ')
		if len(location) > 1:
			city = location[1].strip()
			company_data['city'] = city
	tel_table = soup.find('table', class_='table nomargin')
	if tel_table:
		phone_ele = tel_table.find_all('span', class_='marketingInfoTelFax')
		phone = [tel.text.strip() for tel in phone_ele]
		company_data['phone'] = phone
	fax_elements = soup.find_all('span', class_='mrg-fiche2')
	fax = [fax_element.find_next('span', class_='marketingInfoTelFax').text.strip() for fax_element in fax_elements]
	company_data['fax'] = fax
	email_elements = soup.find_all('span', class_='mrg-fiche3')
	email_links = [email_element.find_next('a')['href'] for email_element in email_elements]
	emails = [link.split(':')[-1] for link in email_links]
	company_data['mail'] = emails
	website_elements = soup.find_all('span', class_='mrg-fiche4')
	website = [website_element.find_next('a')['href'] for website_element in website_elements]
	company_data['website'] = website
	status_tag = soup.find('i', class_='folder-openicon- icon-fw')
	status = status_tag.find_parent('td').find_next_sibling('td').text.strip() if status_tag else None
	company_data['status'] = status
	capital_tag = soup.find('i', class_='moneyicon- icon-fw')
	capital = capital_tag.find_parent('td').find_next_sibling('td').text.strip() if capital_tag else None
	company_data['capital'] = capital
	latitude = soup.find('input', class_='latitude')['value']
	company_data['latitude'] = latitude
	longitude = soup.find('input', class_='longitude')['value']
	company_data['longitude'] = longitude
	activity_tag = soup.find('span', title=True)
	activity = activity_tag.find_next('h2').text.strip() if activity_tag else None
	company_data['activity'] = activity
	return company_data

def process_data(data_dict, outfile):
	data = []
	# with ThreadPoolExecutor(max_workers=num_workers) as executor:
	# 	company_links = [f'{url}{link}' for link in data_dict]
	# 	print("Fetching data for", len(company_links), "companies...")
	# 	futures = {executor.submit(extract_data, link): link for link in company_links}

	# 	for future in concurrent.futures.as_completed(futures):
	# 		data.append(future.result())
	# for link in data_dict:
	# 	data.append(extract_data(f'{url}{link}'))
	        # Multiprocessing pool
	with Pool() as pool:
		args = [f'{url}{link}' for link in data_dict]
		data = pool.map(extract_data, args)

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
	process_data(data_dict, filename)
	time_taken = time.time() - start_time
	print(f"Time taken: {time_taken / 60} minutes")

if __name__ == '__main__':
	main()