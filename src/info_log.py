import json
from time import asctime
import os
import collections

log_name = "activity.log"
sub_log_name = "activity{}.sublog"

def makehash():
    return collections.defaultdict(makehash)

#finds a not used numbered filename, by trying all 
#existing filenames until you find one that doesn't exist 
def make_sub_log_name():
	i = 1
	while i<999:
		try:
			open(sub_log_name.format(i))
			i+=1
		except:
			return sub_log_name.format(i)

class info_log:
	def __init__(self, file_location):
		self.log_location = os.path.join(os.path.dirname(file_location), log_name)
		self.unique_id = file_location.split(os.sep)[-4]
		self.dataset = file_location.split(os.sep)[-3]
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
			log_data[self.unique_id][self.dataset][self.filename].append(event)
		except:
			try:
				log_data[self.unique_id][self.dataset].update({self.filename: [event]})
			except:
				try:
					log_data[self.unique_id][self.dataset] = {self.filename: [event]}
				except:
					try:
						log_data[self.unique_id].update({self.dataset: {self.filename: [event]}})
					except:
						try:
							log_data[self.unique_id] = {self.dataset: {self.filename: [event]}}
						except:
							log_data = {self.unique_id: {self.dataset: {self.filename: [event]}}}
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

	def sub_start(self, name, extra_info=""):
		sub_name = make_sub_log_name()
		self.job("sub_" + name, sub_name)
		return sub_name


def find_logs(path):
	log_locations = []
	for root, dirs, files in os.walk(path):
		for file in files:
			if file.endswith(".log"):
				log_locations.append(os.path.join(root,file))
	return log_locations

def get_combined_log(path):
	log_locations = find_logs(path)
	huge_log = makehash()
	for log_location in log_locations:
		with open(log_location, 'r') as log_file:
			loaded = json.load(log_file)
			for username in loaded.keys():
				for dataset in loaded[username].keys():
					for filename in loaded[username][dataset]:
						for event in loaded[username][dataset][filename]:
							if event["event"].startswith("sub_"):
								with open(os.path.join(os.path.dirname(log_location), event["extra"]), 'r') as sub_log:
									event["extra"] = sub_log.read()
							try:
								# probably never occurs?
								huge_log[username][dataset][filename].append(event)
							except:
								huge_log[username][dataset][filename] = [event]
	return json.loads(json.dumps(huge_log)) #turns the defaultdict into a dict

###example of how to use the log
# log = info_log("/tmp/<user_location>/web_interface/<file_location>/<file_name>")
# print(log.load())
# log.job_start()
# print(json.dumps(log.load(), indent=4))

###example of how to retrieve all the logs
# loc = "tmp/"
# log = get_combined_log(loc)
