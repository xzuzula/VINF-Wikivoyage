import lucene, os, json, re

from java.io import File
from org.apache.lucene import analysis, document, index, queryparser, search, store, util

assert lucene.getVMEnv() or lucene.initVM()

# Lucene setup
analyzer = analysis.standard.StandardAnalyzer()
directory = store.FSDirectory.open(File('dataIndex').toPath())
config = index.IndexWriterConfig(analyzer)
iwriter = index.IndexWriter(directory, config)

ireader = index.DirectoryReader.open(directory)
isearcher = search.IndexSearcher(ireader)

# Tokenized field type
tokenized = document.FieldType()
tokenized.setStored(True)
tokenized.setTokenized(True)
tokenized.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS)

tokenized2 = document.FieldType()
tokenized2.setStored(True)
tokenized2.setTokenized(True)
tokenized2.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

# Non-tokenized field type
non_tokenized = document.FieldType()
non_tokenized.setStored(True)
non_tokenized.setTokenized(False)
non_tokenized.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS)

delete_id = []

def insert_wiki_data(path: str) -> None:
	"""
    Processes and inserts Wikipedia data from JSON files into a Lucene index.

    This function reads JSON files from a specified directory and inserts the data into a Lucene index. Each JSON file is expected to contain Wikipedia data. The function parses the JSON data, constructs a new Lucene document for each entry, and adds it to the index. If a document with the same title exists, it updates the existing document with new information.

    Global variables used:
    - `iwriter`: An IndexWriter object for adding documents to the index.
    - `isearcher`: An IndexSearcher object for searching existing documents.
    - `tokenized` and `non_tokenized`: FieldType objects indicating how fields should be indexed.
    - `delete_id`: A list to track document IDs for deletion.

    Parameters:
    path (str): The directory path containing the JSON files to be processed.

    Returns:
    None: This function does not return anything.

    Raises:
    FileNotFoundError: If the specified path is not found or is not a directory.

    Example:
    Calling `insert_wiki_data("/path/to/json/files")` will process and index all '.json' files in the specified directory.
    """
	global iwriter, isearcher, tokenized, non_tokenized, delete_id, tokenized2
	if not os.path.isdir(path):
		print("Path not found")
		return
	for file in os.listdir(path):
		if file.endswith(".json"):
			with open(path + "/" + file, "r", encoding="utf-8") as json_file:
				for line in json_file:
					json_data = json.loads(line)
					field_data = {json_data["mod_key"][i]: json_data["mod_val"][i] for i in range(len(json_data["mod_key"]))}
					parser = queryparser.classic.QueryParser("title", analyzer)
					query = re.sub(r"[\+\-&\|!\(\){}\[\]\^\"~\*\?:\\\/]+", "", json_data["title"])
					query = "title:\"" + query + "\""
					query = parser.parse(query)
					hits = isearcher.search(query, 100000).scoreDocs
					for hit in hits:
						# print("indexing...")
						doc_id = hit.doc
						old_doc = isearcher.doc(doc_id)
						old_doc_fields = old_doc.getFields()
						new_doc = document.Document()
						old_names = []
						for field in old_doc_fields:
							old_names.append(field.name())
							if field.name() == "id":
								new_doc.add(document.Field("id", "_" + old_doc["id"], non_tokenized))
								delete_id.append(old_doc["id"])
							elif field.name() == "link":
								new_doc.add(document.Field("link", old_doc["link"], non_tokenized))
							elif field.name() == "title":
								new_doc.add(document.Field("title", old_doc["title"], tokenized2))
							elif field.name() == "other":
								new_doc.add(document.Field("other", old_doc["other"], tokenized))
							else:
								new_doc.add(field)
						for field_key in field_data:
							if field_key not in old_names:
								new_doc.add(document.Field(field_key, field_data[field_key], tokenized))
						# iwriter.updateDocument(index.Term("id", old_doc["id"]), new_doc)
						iwriter.addDocument(new_doc)

insert_wiki_data("spark_output")

iwriter.commit()

for item in delete_id:
	iwriter.deleteDocuments(index.Term("id", item))

iwriter.commit()

iwriter.close()
ireader.close()
