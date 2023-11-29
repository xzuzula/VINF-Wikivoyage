import lucene

from java.io import File
from org.apache.lucene import analysis, document, index, queryparser, search, store, util

assert lucene.getVMEnv() or lucene.initVM()

# Lucene setup
analyzer = analysis.standard.StandardAnalyzer()
directory = store.FSDirectory.open(File('dataIndex').toPath())

ireader = index.DirectoryReader.open(directory)
isearcher = search.IndexSearcher(ireader)

parser = queryparser.classic.QueryParser("title", analyzer)
# query_str = 'categories:Europe AND link:https\:\/\/en.*'  10
# query_str = 'title:slovakia AND link:https\:\/\/en.*' Sleep 1
# query_str = 'capital:Prague AND link:https\:\/\/en.*' title 1
# Query index
while True:
    query_str = input("Zadaj query: ")
    number_hits = int(input("Zadaj limit vysledkov: "))
    query = parser.parse(query_str)
    hits = isearcher.search(query, number_hits).scoreDocs
    output_field = input("Vystupne pole: ")
    # print("Pocet vysledkov:", len(hits))
    # Iterate through the results:
    for hit in hits:
        hitDoc = isearcher.doc(hit.doc)
        fields = hitDoc.getFields()
        if output_field == "all_fields":
            for field in fields:
                print(field.name())
        else:
            print(hitDoc[output_field])

ireader.close()
directory.close()
