import lucene, os, json, re

from java.io import File
from org.apache.lucene import analysis, document, index, queryparser, search, store, util

assert lucene.getVMEnv() or lucene.initVM()

analyzer = analysis.standard.StandardAnalyzer()
directory = store.FSDirectory.open(File('dataIndex').toPath())
config = index.IndexWriterConfig(analyzer)
iwriter = index.IndexWriter(directory, config)

ireader = index.DirectoryReader.open(directory)
isearcher = search.IndexSearcher(ireader)

tokenized = document.FieldType()
tokenized.setStored(True)
tokenized.setTokenized(True)
tokenized.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS)

def insert_wiki_data(path: str) -> None:
	global iwriter, isearcher, tokenized
	if not os.path.isdir(path):
		print("Path not found")
		return
	for file in os.listdir(path):
		if file.endswith(".json"):
			with open(path + "/" + file, "r", encoding="utf-8") as json_file:
				for line in json_file:
					json_data = json.loads(line)
					field_data = {json_data["mod_key"][i]: json_data["mod_val"][i] for i in range(len(json_data["mod_key"]))}
					new_doc = document.Document()
					new_doc.add(document.Field("title", json_data["title"], tokenized))
					for field_key in field_data:
						new_doc.add(document.Field(field_key, field_data[field_key], tokenized))
					iwriter.addDocument(new_doc)

insert_wiki_data("spark_output")

iwriter.commit()

iwriter.close()
ireader.close()
