import lucene, re, os, json

from java.io import File
from org.apache.lucene import analysis, document, index, queryparser, search, store

assert lucene.getVMEnv() or lucene.initVM()

# Sets up the Lucene analyzer, directory for storing the index, and the IndexWriter configuration.
analyzer = analysis.standard.StandardAnalyzer()
directory = store.FSDirectory.open(File('dataIndex').toPath())
config = index.IndexWriterConfig(analyzer)
iwriter = index.IndexWriter(directory, config)
all_data = {}
doc_counter = 0

# https://lucenetutorial.com/lucene-in-5-minutes.html
# https://coady.github.io/lupyne/

# Regex for removing html tags
CLEANR = re.compile('<.*?>')

# Get crawled data names
web_files = os.listdir("data/")
# web_files = ["9f7a2ffdc4ad418839e505ac46de101f.txt"]
# print(web_files)

def load_objects(path="res_mod.txt") -> list[str]:
	"""
    Reads and extracts objects from a specified text file.

    This function opens a text file, reads its contents, and extracts a list of objects based on a predefined regular expression. Each object is assumed to be enclosed in single quotes within the file.

    Parameters:
    path (str, optional): The path to the text file to be read. Defaults to "res_mod.txt".

    Returns:
    list[str]: A list of strings, where each string is an object extracted from the file.

    Raises:
    FileNotFoundError: If the specified file does not exist.
    IOError: If there is an error in reading the file.
    """
	obj_regex = re.compile("\'(.*)\'")
	result_obj = []
	with open(path, "r", encoding="utf-8") as obj_file:
		file_text = obj_file.read()
		result_obj = re.findall(obj_regex, file_text)
	return result_obj

all_objs = load_objects()

def extract_data(data: str) -> dict:
	"""
	Extracts and processes data from a given HTML string.

	This function parses the input HTML data to extract various elements such as title, link, categories, and paragraphs. It uses regular expressions to clean HTML content and extract structured information. The function also identifies and processes specific objects listed in the global variable 'all_objs'.

	Parameters:
	data (str): A string containing HTML content to be processed.

	Returns:
	dict: A dictionary containing the extracted data. The keys include 'title', 'link', 'num_cat' (number of categories), 'categories' (a list of category names), 'paragraphs' (a dictionary with keys as object names and values as corresponding content), and 'other' (remaining content after extraction).

	Note:
	- The 'title' is extracted from the <title> tag.
	- The 'link' is derived from the first line of the data.
	- Categories are extracted from a specific span with the class 'ext-geocrumbs-breadcrumbs'.
	- Paragraphs are extracted based on the objects in 'all_objs', each limited to 32000 characters.
	- Any HTML tags are removed from the extracted content.
	"""
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
	"""
    Writes the keys of a given dictionary to a file named 'gazetteer.txt'.

    This function takes a dictionary, extracts its keys, and writes them line by line to a text file named 'gazetteer.txt'. Each key is written on a new line. The file is encoded in UTF-8.

    Parameters:
    data (dict): The dictionary whose keys are to be written to the file.

    Returns:
    None: This function does not return anything.

    Raises:
    IOError: If there is an error in writing to the file.

    Example:
    Given a dictionary:
    {'key1': 'value1', 'key2': 'value2'}
    The function will create a file 'gazetteer.txt' with the following content:
    key1
    key2
    """
	data_keys = list(data.keys())
	with open("gazetteer.txt", "w", encoding="utf-8") as gaz_file:
		for item in data_keys:
			gaz_file.write(item + "\n")

def get_all_data(extracted_data: dict) -> dict:
	"""
    Processes and cleans data from an extracted data dictionary.

    This function cleans the 'title' and each 'category' in the extracted data by removing any trailing content after specific symbols ('-', '(', '|', '–', '—') and newline characters. It then compiles a dictionary with these cleaned items as keys, each associated with the value True.

    If the 'extracted_data' dictionary is empty, it returns an empty dictionary.

    Parameters:
    extracted_data (dict): A dictionary containing 'title' and 'categories'. The 'title' is a string and 'categories' is a list of strings.

    Returns:
    dict: A dictionary with cleaned 'title' and 'categories' as keys, each mapped to True. Returns an empty dictionary if 'extracted_data' is empty.

    Example:
    Given 'extracted_data':
    {'title': 'Example Title - Extra Info', 'categories': ['Category1 (Detail)', 'Category2']}
    The function will return:
    {'Example Title': True, 'Category1': True, 'Category2': True}
    """
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
	"""
    Inserts data into a Lucene document and adds it to the index.

    This function constructs a Lucene document by adding various fields from the provided data dictionary. It handles different types of data fields, including tokenized and non-tokenized text, integers, and floats. The fields 'categories', 'paragraphs', 'link', 'title', 'other', and 'id' are specifically processed. Tokenized fields are searchable, while non-tokenized fields are stored but not searchable.

    The constructed document is then added to a global index writer ('iwriter') for indexing.

    Parameters:
    data (dict): A dictionary containing data to be added to the Lucene document. The keys should correspond to the field names, and the values to the field content.

    Returns:
    None: This function does not return anything.

    Raises:
    IOError: If there is an issue in writing the document to the index.

    Example:
    Given 'data' as:
    {
        'title': 'Example Title', 
        'link': 'http://example.com', 
        'categories': ['Category1', 'Category2'],
        'paragraphs': {'Para1': 'Text1', 'Para2': 'Text2'},
        'other': 'Additional Info',
        'id': '12345'
    }
    The function will process and add these as fields to a Lucene document.
    """
	global iwriter
	doc = document.Document()
	tokenized = document.FieldType()
	tokenized.setStored(True)
	tokenized.setTokenized(True)
	tokenized.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS)

	tokenized2 = document.FieldType()
	tokenized2.setStored(True)
	tokenized2.setTokenized(True)
	tokenized2.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

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
			doc.add(document.Field(key, data[key], tokenized2))
		elif key == "other":
			doc.add(document.Field(key, data[key], tokenized))
		elif key == "id":
			doc.add(document.Field(key, data[key], non_tokenized))
		else:
			continue
	iwriter.addDocument(doc)


if __name__ == '__main__':
	for web in range(len(web_files)):
		with open(f"data/{web_files[web]}", "r", encoding="utf-8") as file_content:
			txt_file = file_content.read()
			ex_data = extract_data(txt_file)
			ex_data["id"] = str(doc_counter)
			all_data.update(get_all_data(ex_data))
			# print(ex_data)
			insert_data(ex_data)
		doc_counter += 1
	create_gazetteer(all_data)

iwriter.close()
