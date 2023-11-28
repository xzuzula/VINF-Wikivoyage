import json

"""
Updates the list of scraped URLs and their hash mappings.

This code reads a JSON file containing lists of 'wikivoyage' and 'other' URLs. It converts these lists into dictionaries to remove duplicates and ensure uniqueness.
Each URL is used as a key in the dictionary with the value set to True.

After processing, it updates the original dictionary with these unique lists and their corresponding hash mappings, then writes this updated dictionary back to a different JSON file.

The code updates the JSON file with the deduplicated lists of URLs and their hash mappings.

File operations:
- Reads from 'history/link_queue – kópia.json'.
- Writes to 'history/link_queue_cl.json'.

Note:
- The purpose of this code is to clean and update the list of scraped URLs for further processing.
"""

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
