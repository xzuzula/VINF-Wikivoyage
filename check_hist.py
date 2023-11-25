import json, os, hashlib
import urllib.parse

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
