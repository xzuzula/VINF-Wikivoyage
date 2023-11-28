import re, os

heading_regex = "<span class=\"mw-headline\"[^>]*>([^<]*)</span>"

entity_count = {}

web_files = os.listdir("data/")
# print(web_files)

# Counts occurrences of headings matching a regex in a list of files.
for web in web_files:
	txt_file = ""
	with open(f"data/{web}", "r", encoding="utf-8") as file_content:
		txt_file = file_content.read()
		headings = re.findall(heading_regex, txt_file)
		for heading in headings:
			if entity_count.get(heading) is not None:
				entity_count[heading] += 1
			else:
				entity_count[heading] = 1

sorted_entity = list(entity_count.items())
sorted_entity.sort(key=lambda item: item[1], reverse=True)

# Writes occurrences to txt file
with open("res.txt", "w", encoding="utf-8") as file_res:
	for i in sorted_entity:
		file_res.write(str(i) + "\n")
