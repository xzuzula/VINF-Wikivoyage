import json

scraped_urls = None

with open("history/link_queue – kópia.json", "r", encoding="utf-8") as history_file:
	scraped_urls = json.load(history_file)

wikivoyage_hash = dict.fromkeys(scraped_urls["wikivoyage"], True)
other_hash = dict.fromkeys(scraped_urls["other"], True)

scraped_urls["wikivoyage"] = list(wikivoyage_hash)
scraped_urls["other"] = list(other_hash)
scraped_urls["wikivoyage_hash"] = wikivoyage_hash
scraped_urls["other_hash"] = other_hash

with open("history/link_queue_cl.json", "w", encoding="utf-8") as json_file:
	json.dump(scraped_urls, json_file, ensure_ascii=False)
