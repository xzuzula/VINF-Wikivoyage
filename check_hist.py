import json, os, hashlib
import urllib.parse

"""
Processes scraped URLs and updates their status based on whether they have already been visited.

This code reads a JSON file containing previously scraped URLs, categorizes them into 'wikivoyage' and 'other' URLs, and checks each URL against a list of files in a directory to determine if they have been visited.
URLs that have not been visited are added to new dictionaries, while visited URLs are marked accordingly in the 'visited' category.

The processed URLs are then written back to a new JSON file with updated information.

The code updates the JSON file with the current status of each URL.

Note:
- URL encoding is performed to handle special characters.
- MD5 hashing is used to generate a unique identifier for each URL.

File operations:
- Reads from 'history/link_queue_cl.json'.
- Writes to 'history/link_queue_chck.json'.
"""

scraped_urls = None
new_wikivoyage_urls = {}
new_other_urls = {}

with open("history/link_queue_cl.json", "r", encoding="utf-8") as history_file:
	scraped_urls = json.load(history_file)

hex_files = dict.fromkeys(os.listdir("data"), True)

for i in range(len(scraped_urls["wikivoyage"])):
	web_url = scraped_urls["wikivoyage"][i]
	web_url = urllib.parse.quote(web_url, "<>=/:!")
	hash_val = hashlib.md5(web_url.encode('utf-8')).hexdigest() + ".txt"
	if hex_files.get(hash_val) is None:
		new_wikivoyage_urls[web_url] = True
	else:
		scraped_urls["visited"][web_url] = True

for i in range(len(scraped_urls["other"])):
	web_url = scraped_urls["other"][i]
	web_url = urllib.parse.quote(web_url, "<>=/:!")
	hash_val = hashlib.md5(web_url.encode('utf-8')).hexdigest() + ".txt"
	if hex_files.get(hash_val) is None:
		new_other_urls[web_url] = True
	else:
		scraped_urls["visited"][web_url] = True

scraped_urls["wikivoyage"] = list(new_wikivoyage_urls)
scraped_urls["other"] = list(new_other_urls)
scraped_urls["wikivoyage_hash"] = new_wikivoyage_urls
scraped_urls["other_hash"] = new_other_urls

with open("history/link_queue_chck.json", "w", encoding="utf-8") as json_file:
	json.dump(scraped_urls, json_file, ensure_ascii=False)
