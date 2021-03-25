from botocore.exceptions import ClientError
from pathlib import Path
import subprocess
import requests
import argparse
import boto3
import json
import csv
import os

# csv_url = https://raw.githubusercontent.com/woocommerce/woocommerce/master/sample-data/sample_products.csv

# create an 'output.csv' file if it does not exist yet 
def find(name, list_path):
	file_path = ''.join((list_path, name))
	my_file = Path(file_path)

	if os.path.isfile(name):
		print(f'\n>> {name} exists\n')
	else:
		print(f'\n>> Does not exist.\nCreating {name}...\n')
		subprocess.run(['touch', name])

# download csv file from user input url
def get_csv(csv_url):
	# csv_url = input('Please input the url of the Products CSV: ')

	with requests.Session() as req:
		try:
			response = req.get(csv_url)
			response.raise_for_status()

			return response

		except HTTPError as http_err:
			print(f'HTTP error occurred: {http_err}')
		except Exception as err:
			print(f'Error occurred: {err}')

def readAndWrite(csv_url):
	try:
		base_path = '/home/kent/Desktop/python/python_capstone/'
		file_name = 'output.csv'

		find(file_name, base_path)

		dir_path = ''.join((base_path, file_name))

		response = get_csv(csv_url)

		with open(dir_path, 'w', newline='') as out:				
			rdr = csv.reader(response.text.strip().split('\n'))
			fieldnames = next(rdr)

			reader = csv.DictReader(response.text.strip().split('\n'))
			writer = csv.DictWriter(out, fieldnames=fieldnames)

			# write products with categories to 'output.csv'
			writer.writeheader()

			for row in reader:
				if row['Categories']:		
					writer.writerow(row)

		message = ' '.join(('Done processing CSV with URL:', csv_url))
		source_application = os.path.basename(__file__)

		print(f'>> message: {message}')
		print(f'>> source_application: {source_application}\n')

		temp = {
			"key1": "INFO",
			"key2": message,
			"key3": "",
			"key4": source_application
		}
			
		payload = json.dumps(temp)

		callLoggingAndNotifAPI(payload)
	except Exception as err:
		message = ' '.join(('Failed to process CSV with URL:', csv_url))
		source_application = os.path.basename(__file__)

		print(f'>> message: {message}')
		print(f'>> source_application: {source_application}\n')

		temp = {
			"key1": "ERROR",
			"key2": message,
			"key3": "",
			"key4": source_application
		}
			
		payload = json.dumps(temp)
		callLoggingAndNotifAPI(payload)

def callLoggingAndNotifAPI(payload):
	response = requests.post(
		url="https://qm5etw0909.execute-api.ap-southeast-1.amazonaws.com/default/kent-capstone",
		headers={'x-api-key': 'LKTJdj3oNC6qSjKHeXQb1azDUcSxN1cq1VvrPPls'},
		data=payload
	)

	print(f'{response.json()}\n')


if __name__ == '__main__':	
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()

	csv_get = subparsers.add_parser('csv_get')
	csv_get.set_defaults(func=get_csv)

	csv_readAndWrite = subparsers.add_parser('csv_readAndWrite')
	csv_readAndWrite.add_argument('csv_url', help='URL to CSV file.')
	csv_readAndWrite.set_defaults(func=readAndWrite)	

	args = parser.parse_args()

	if hasattr(args, 'func'):
		if args.func.__name__ == 'get_csv':
			args.func(csv_url=args.csv_url)
		elif args.func.__name__ == 'readAndWrite':
			args.func(csv_url=args.csv_url)