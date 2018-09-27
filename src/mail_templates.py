import requests

FROM_ADDRESS = "noreply@cattle.datalegend.net"

def send_simple_message(to_address, name, auth_token):
	return requests.post(
		"https://api.mailgun.net/v3/mailgun.nimbostratus.nl/messages",
		auth=("api", auth_token),
		data={"from": FROM_ADDRESS,
			"to": to_address,
			"subject": "Hello {}".format(name),
			"text": "This is a message."})

def send_new_graph_message(to_address, username, csv_filenames, auth_token):
	text = "Congratulations {},\n\nCattle has created new graphs using these csv files:\n\n".format(username)
	for csv_filename in csv_filenames:
		text += csv_filename+"\n"
	text += "\nWith kind regards,\n   Cattle"
	return requests.post(
		"https://api.mailgun.net/v3/mailgun.nimbostratus.nl/messages",
		auth=("api", auth_token),
		data={"from": FROM_ADDRESS,
			"to": to_address,
			"subject": "a message from Cattle!",
			"text": text})