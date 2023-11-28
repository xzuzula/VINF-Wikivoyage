import lucene, re
import urllib.request
import urllib.parse
import numpy as np

from sklearn.metrics import average_precision_score, recall_score

from java.io import File
from org.apache.lucene import analysis, document, index, queryparser, search, store, util

assert lucene.getVMEnv() or lucene.initVM()

# Lucene setup
analyzer = analysis.standard.StandardAnalyzer()
directory = store.FSDirectory.open(File('dataIndex').toPath())

ireader = index.DirectoryReader.open(directory)
isearcher = search.IndexSearcher(ireader)

def load_keywords(path="eval_dataset.txt") -> list[str]:
    words = []
    with open(path, "r", encoding='utf-8') as data_file:
        for line in data_file:
            words.append(line.strip())
    return list(set(words))

def get_wiki_res(data: list[str]) -> dict:
    result = {}
    req_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    total_regex = r"data-mw-num-results-total=\"(.*?)\""
    total_count = 0
    for title in data:
        web_url = 'https://en.wikivoyage.org/w/index.php?search=intitle:"' + title +\
        '"&title=Special:Search&profile=advanced&fulltext=1&advancedSearch-current={"fields":{"intitle":"' + title + '"}}&ns0=1'
        web_url = urllib.parse.quote(web_url, "<>=/:!?&")
        # print(web_url)
        req = urllib.request.Request(web_url, headers=req_headers)
        content = urllib.request.urlopen(req, timeout=5).read().decode('utf-8')
        try:
            total_count = int(re.findall(total_regex, content)[0])
        except:
            pass
        result[title] = total_count
    return result

def get_index_res(data: list[str]) -> dict:
    result = {}
    parser = queryparser.classic.QueryParser("title", analyzer)
    # Query index
    for title in data:
        query = parser.parse("title:" + title + " AND link:https\\:\\/\\/en.*")
        hits = isearcher.search(query, 1000000).scoreDocs
        result[title] = len(hits)
    return result

keywords = load_keywords()
wiki_res = get_wiki_res(keywords)
wiki_count = np.array(list(wiki_res.values())).reshape(-1, 1)
index_res = get_index_res(keywords)
index_count = np.array(list(index_res.values())).reshape(-1, 1)

precision = average_precision_score(wiki_count, index_count, average="macro")
recall = recall_score(wiki_count, index_count, average="macro", zero_division=0.0)

print(f"Average precision: {precision * 100} %")
print(f"Average recall: {recall * 100} %")

ireader.close()
directory.close()
