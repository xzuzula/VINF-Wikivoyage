import lucene, re, os, json

from java.io import File
from org.apache.lucene import analysis, document, index, queryparser, search, store

assert lucene.getVMEnv() or lucene.initVM()

analyzer = analysis.standard.StandardAnalyzer()
directory = store.FSDirectory.open(File('dataIndex').toPath())
config = index.IndexWriterConfig(analyzer)
iwriter = index.IndexWriter(directory, config)
all_data = {}
doc_counter = 0

# https://lucenetutorial.com/lucene-in-5-minutes.html

# CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
CLEANR = re.compile('<.*?>')

web_files = os.listdir("data/")
# web_files = ["9f7a2ffdc4ad418839e505ac46de101f.txt"]
# print(files)

def load_objects(path="res_mod.txt") -> list[str]:
	obj_regex = re.compile("\'(.*)\'")
	result_obj = []
	with open(path, "r", encoding="utf-8") as obj_file:
		file_text = obj_file.read()
		result_obj = re.findall(obj_regex, file_text)
	return result_obj

all_objs = load_objects()

def extract_data(data: str) -> dict:
	global all_objs
	clean_html = ""
	rest = ""
	tree_items = []
	paragraph = []
	paragraphs = {}
	title = re.findall("<title>(.*)</title>", data)
	link = data.split("\n")[0]
	if len(title) == 0:
		title = ""
	else:
		title = title[0]
	try:
		tree = re.findall("<span class=\"ext-geocrumbs-breadcrumbs\">.*", data)[0]
		tree_items = re.findall("title=\"(.*?)\"", tree)
		tree_items.append(re.findall("<bdi>(.*?)</bdi>", tree)[-1])
	except:
		pass
	clean_html = re.sub(CLEANR, "", data)
	"""
	with open("test.txt", "w", encoding='utf-8') as test_file:
		test_file.write(clean_html)
	"""
	for obj in range(len(all_objs)):
		paragraph = re.findall(f"({all_objs[obj]}\\[edit\\].*?(?=\\w+\\[edit\\]))", clean_html, flags=re.DOTALL)
		if obj == 0:
			rest = re.sub(f"({all_objs[obj]}\\[edit\\].*?(?=\\w+\\[edit\\]))", "", clean_html, flags=re.DOTALL)
		else:
			rest = re.sub(f"({all_objs[obj]}\\[edit\\].*?(?=\\w+\\[edit\\]))", "", rest, flags=re.DOTALL)
		if len(paragraph) >= 1:
			prettify_paragraph = paragraph[0]
			prettify_paragraph = re.sub("\n+", "\n", prettify_paragraph)
			paragraphs[all_objs[obj]] = prettify_paragraph[:32000]
	rest = re.sub("\n+", "\n", rest)
	return {"title": title, "link": link, "num_cat": len(paragraphs.keys()), "categories": tree_items, "paragraphs": paragraphs, "other": rest[:32000]}

def create_gazetteer(data: dict) -> None:
	data_keys = list(data.keys())
	with open("gazetteer.txt", "w", encoding="utf-8") as gaz_file:
		for item in data_keys:
			gaz_file.write(item + "\n")

def get_all_data(extracted_data: dict) -> dict:
	if len(list(extracted_data.keys())) == 0:
		return {}
	all_ex_data = {}
	mod_title = re.sub(r"( -.*)|( \(.*)|( \|.*)|( –.*)|( —.*)|(\n)", "", extracted_data["title"])
	all_ex_data[mod_title] = True
	for category in extracted_data["categories"]:
		new_cat = re.sub(r"( -.*)|( \(.*)|( \|.*)|( –.*)|( —.*)|(\n)", "", category)
		all_ex_data[new_cat] = True
	return all_ex_data

def insert_data(data: dict) -> None:
	global iwriter
	doc = document.Document()
	tokenized = document.FieldType()
	tokenized.setStored(True)
	tokenized.setTokenized(True)
	tokenized.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS)
	non_tokenized = document.FieldType()
	non_tokenized.setStored(True)
	non_tokenized.setTokenized(False)
	non_tokenized.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS)
	for key in data:
		if key == "categories":
			for i in range(len(data[key])):
				doc.add(document.Field("categories", data[key][i], non_tokenized))
		elif key == "paragraphs":
			for new_key in data[key]:
				doc.add(document.Field(new_key, data[key][new_key], tokenized))
		elif type(data[key]) == int:
			doc.add(document.IntPoint(key, data[key]))
		elif type(data[key]) == float:
			doc.add(document.FloatPoint(key, data[key]))
		elif key == "link":
			doc.add(document.Field(key, data[key], non_tokenized))
		elif key == "title":
			doc.add(document.Field(key, data[key], tokenized))
		elif key == "other":
			doc.add(document.Field(key, data[key], tokenized))
		elif key == "id":
			doc.add(document.Field(key, data[key], non_tokenized))
		else:
			continue
	iwriter.addDocument(doc)


if __name__ == '__main__':
	for web in range(len(web_files)):
		# for web in range(1):
		# print(web_files[web])
		with open(f"data/{web_files[web]}", "r", encoding="utf-8") as file_content:
			txt_file = file_content.read()
			ex_data = extract_data(txt_file)
			ex_data["id"] = str(doc_counter)
			# all_data = get_all_data(ex_data)
			# print(ex_data)
			insert_data(ex_data)
		doc_counter += 1
	# create_gazetteer(all_data)

iwriter.close()
