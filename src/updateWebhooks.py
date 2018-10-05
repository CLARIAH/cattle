# can be used to check all the datasets for each organisation
# a given user is a member for the existence of a webhook.
# if a dataset does not have a webhook yet one will be created.

import argparse
import requests
import json

#find all organisations <username> is a member of.
def orgs_of_user(username):
	druid_url = 'https://api.druid.datalegend.net/accounts/{}/orgs'.format(username)
	orgs_json = requests.get(druid_url).json()

	orgs = []
	for orgs_i in range(0, len(orgs_json)):
		orgs.append(orgs_json[orgs_i]['accountName'])
	return orgs

#find all datasets of one organisation
def datasets_of_org(org):
	org_url = 'https://api.druid.datalegend.net/datasets/{}'.format(org)
	datasets_json = requests.get(org_url).json()

	datasets = []
	for datasets_i in range(0, len(datasets_json)):
		datasets.append(datasets_json[datasets_i]['name'])
	return datasets

#find all datasets for multiple organisations
def datasets_of_orgs(orgs):
	datasets_dict = {}
	for org in orgs:
		datasets_dict[org] = datasets_of_org(org)
	return datasets_dict

#return the number of hooks in one dataset
def hooks_in_dataset(org, dataset, API_token):
	header = {"Cookie": "jwt={}".format(API_token)}
	hooks_url = 'https://api.druid.datalegend.net/datasets/{}/{}/hooks/'.format(org, dataset)

	datasets_info = requests.get(hooks_url, headers=header)
	return len(datasets_info.json())

#remove all the datasets that already have a webhook
def remove_hooked(datasets_dict, API_token):
	new_datasets_dict = {}
	for org in datasets_dict.keys():
		for dataset in datasets_dict[org]:
			if hooks_in_dataset(org, dataset, API_token) == 0:
				try:
					new_datasets_dict[org].append(dataset)
				except:
					new_datasets_dict[org] = [dataset]
	return new_datasets_dict

# check for existing files inside of the datasets
def check_dataset(org, dataset):
	dataset_url = "https://api.druid.datalegend.net/datasets/{}/{}/assets".format(org, dataset)
	files_json = requests.get(dataset_url).json()

	# print(files_json)

	files = []
	for file_i in range(0, len(files_json)):
		files.append(files_json[file_i]["assetName"])
	# print(files)
	return files

#starts the druid interface of cattle in the specified dataset
def call_cattle(org, dataset):
	cattle_url = "http://cattle.datalegend.net/druid/{}/{}".format(org, dataset)
	print("calling Cattle for {}".format(cattle_url))
	requests.post(cattle_url)

#check if the datasets without webhooks are still empty
#if there are files present cattle will be called to handle those.
def check_datasets(datasets_dict):
	for org in datasets_dict.keys():
		for dataset in datasets_dict[org]:
			files = check_dataset(org, dataset)
			if len(files) > 0:
				call_cattle(org, dataset)

#add a new webhook to the specified dataset
def add_hook_to_dataset(org, dataset, API_token):
	cattle_url = "http://cattle.datalegend.net/druid/{}/{}".format(org, dataset)
	payload_dict = {"active":True, "onEvents":{"fileUpload":True, "graphImport":False,"linkedDataUpload":False},"payloadFormat":"JSON","url":cattle_url}
	payload = json.dumps(payload_dict)

	header = {"Content-Type": "application/json", "Cookie": "jwt={}".format(API_token)}
	druid_url = 'https://api.druid.datalegend.net/datasets/{}/{}/hooks/'.format(org, dataset)

	requests.post(druid_url, data=payload, headers=header)
	print("succesfully added a new webhook to \"{}\"".format(dataset))

def add_hook_to_datasets(datasets_dict, API_token):
	for org in datasets_dict.keys():
		for dataset in datasets_dict[org]:
			add_hook_to_dataset(org, dataset, API_token)

#main function
def update_webhooks(username, API_token):
	orgs = orgs_of_user(username)
	print("found {} organizations where {} is a member.".format(len(orgs), username))
	datasets_dict = datasets_of_orgs(orgs)
	datasets_dict = remove_hooked(datasets_dict, API_token)
	check_datasets(datasets_dict)
	add_hook_to_datasets(datasets_dict, API_token)


# if __name__ == '__main__':
# 	parser = argparse.ArgumentParser(description="")
# 	parser.add_argument('--username', dest='username', default=None, type=str, help="An user that is a member of every organisation whose datasets will be updated with webhooks")
# 	parser.add_argument('--API_token', dest='API_token', default=None, type=str, help="The API token of the user")
# 	args = parser.parse_args()

# 	if args.username == None or args.API_token == None:
# 		print("unauthorized access")
# 	else:
# 		update_webhooks(args.username, args.API_token)