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

non_tokenized = document.FieldType()
non_tokenized.setStored(True)
non_tokenized.setTokenized(False)
non_tokenized.setIndexOptions(index.IndexOptions.DOCS_AND_FREQS)

delete_id = []

def insert_wiki_data(path: str) -> None:
	global iwriter, isearcher, tokenized, non_tokenized, delete_id
	if not os.path.isdir(path):
		print("Path not found")
		return
	counter = 1
	for file in os.listdir(path):
		if file.endswith(".json"):
			with open(path + "/" + file, "r", encoding="utf-8") as json_file:
				for line in json_file:
					json_data = json.loads(line)
					field_data = {json_data["mod_key"][i]: json_data["mod_val"][i] for i in range(len(json_data["mod_key"]))}
					parser = queryparser.classic.QueryParser("title", analyzer)
					# query = parser.parse("title:" + re.sub(r"[\+\-&\|!\(\){}\[\]\^\"~\*\?:\\\/]+", "", json_data["title"]) + "*" + " AND link:https\:\/\/en.*")
					query = parser.parse(re.sub(r"[\+\-&\|!\(\){}\[\]\^\"~\*\?:\\\/]+", "", json_data["title"]) + "*")
					# query = parser.parse(search.TermQuery(index.Term("title", json_data["title"])))
					hits = isearcher.search(query, 100000).scoreDocs
					for hit in hits:
						# print("indexing...")
						doc_id = hit.doc
						old_doc = isearcher.doc(doc_id)
						old_doc_fields = old_doc.getFields()
						new_doc = document.Document()
						for field in old_doc_fields:
							if field.name() == "id":
								new_doc.add(document.Field("id", "_" + old_doc["id"], non_tokenized))
								delete_id.append(old_doc["id"])
							elif field.name() == "link":
								new_doc.add(document.Field("link", old_doc["link"], non_tokenized))
							elif field.name() == "title":
								new_doc.add(document.Field("title", old_doc["title"], tokenized))
							elif field.name() == "other":
								new_doc.add(document.Field("other", old_doc["other"], tokenized))
							else:
								new_doc.add(field)
						for field_key in field_data:
							if field_key not in old_doc_fields:
								new_doc.add(document.Field(field_key, field_data[field_key], tokenized))
						# all_new_docs.append(new_doc)
						# iwriter.deleteDocuments(index.Term("id", old_doc["id"]))
						# iwriter.updateDocument(index.Term("id", old_doc["id"]), new_doc)
						# if (counter % 100) == 0:
						# iwriter.commit()
						# counter += 1
						# iwriter.deleteDocuments(index.Term("id", old_doc["id"]))
						iwriter.addDocument(new_doc)

insert_wiki_data("spark_output")

iwriter.commit()

for item in delete_id:
	iwriter.deleteDocuments(index.Term("id", item))

iwriter.commit()

iwriter.close()
ireader.close()
