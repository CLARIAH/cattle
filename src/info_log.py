import json
from time import asctime
import os

class info_log:
	def __init__(self, file_location, unique_id=""):
		self.log_location = os.path.join(os.path.dirname(file_location), "activity.log")
		if unique_id == "":
			self.unique_id = file_location.split(os.sep)[2]
		else:
			self.unique_id = unique_id
		self.filename = os.path.basename(file_location)

	def load(self):
		try:
			with open(self.log_location) as json_log:
				return json.load(json_log)
		except:
			return {}

	def save(self, data):
		with open(self.log_location, 'w') as json_log:
			json.dump(data, json_log)

	def add_event(self, event):
		log_data = self.load()
		try:
			log_data[self.unique_id][self.filename].append(event)
		except:
			try:
				log_data[self.unique_id].update({self.filename: [event]})
			except:
				log_data[self.unique_id] = {self.filename: [event]}
		self.save(log_data)

	def job(self, job_name, extra_info=""):
		if extra_info == "":
			self.add_event({"event": job_name, "time": asctime()})
		else:
			self.add_event({"event": job_name, "time": asctime(), "extra": extra_info})

	def job_start(self, extra_info=""):
		self.job("started", extra_info)

	def job_build(self, extra_info=""):
		self.job("build", extra_info)

	def job_conversion(self, extra_info=""):
		self.job("conversion", extra_info)

	def job_end(self, extra_info=""):
		self.job("finished", extra_info)

	def job_error(self, extra_info=""):
		self.job("ERROR", extra_info)


def find_logs(path):
	log_locations = []
	for root, dirs, files in os.walk(path):
		for file in files:
			if file.endswith(".log"):
				log_locations.append(os.path.join(root,file))
	return log_locations

def get_combined_log(path):
	log_locations = find_logs(path)
	huge_log = {}
	for log_location in log_locations:
		with open(log_location, 'r') as log_file:
			loaded = json.load(log_file)
			new_keys = list(loaded.keys())
			copy_new_keys = new_keys[:]
			for key in huge_log.keys():
				for new_key in new_keys:
					if key == new_key:
						huge_log[key].update(loaded[new_key])
						copy_new_keys.remove(key)
						break
			for new_key in copy_new_keys:
				huge_log[new_key] = loaded[new_key]
			# huge_log.update(json.load(log_file))
	return huge_log

###example of how to use the log
# log = info_log("/tmp/<user_location>/web_interface/<file_location>/<file_name>")
# print(log.load())
# log.job_start()
# print(json.dumps(log.load(), indent=4))

###example of how to retrieve all the logs
# loc = "tmp/"
# log = get_combined_log(loc)
