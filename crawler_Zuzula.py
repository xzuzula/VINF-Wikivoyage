import time, re, os, random, hashlib, json, threading
import urllib.request
import urllib.parse
from collections import deque


# Global vars
exit_program = False


class CrawlerInterrupter(threading.Thread):

	def run(self):
		global exit_program
		prompt = input("Enter command: ")
		if prompt == "stop":
			exit_program = True


class Crawler():

	def __init__(self):
		self.base_url = "https://en.wikivoyage.org/wiki/"
		self.req_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
		}
		self.url_regex = r"(https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])"
		self.relative_url_regex = r"href=\"(/.*?)\""
		self.scraped_urls = {"wikivoyage": deque(), "wikivoyage_hash": {}, "other": deque(), "other_hash": {}, "visited": {}}
		self.check_history()

	def check_history(self, file_name="history/link_queue.json") -> None:
		if os.path.isfile(file_name):
			with open(file_name, "r", encoding="utf-8") as history_file:
				self.scraped_urls = json.load(history_file)
				self.scraped_urls["wikivoyage"] = deque(self.scraped_urls["wikivoyage"])
				self.scraped_urls["other"] = deque(self.scraped_urls["other"])
		else:
			self.load_countries()

	def load_countries(self, file_name="countries.txt") -> None:
		with open(file_name, "r", encoding="utf-8") as countries_file:
			for line in countries_file:
				self.scraped_urls["wikivoyage"].append(self.base_url + line.strip())
				self.scraped_urls["wikivoyage_hash"][self.base_url + line.strip()] = True

	def url_format(self, url_tuple: tuple) -> str:
		new_url = url_tuple[1:]
		str_url = ''.join(new_url)
		str_url = (url_tuple[0] + "://" + str_url)
		regexp = re.compile(r'\.png|\.jpg|\.jpeg|\.gif|\.svg|\.pdf|\.docx')
		if regexp.search(str_url):
			return ""
		return str_url

	def rel_url_format(self, root_url: str, url: str) -> str:
		root = re.findall(r"(https://.*?)/", root_url)[0]
		str_url = (root + url)
		regexp = re.compile(r'\.png|\.jpg|\.jpeg|\.gif|\.svg|\.pdf|\.docx')
		if regexp.search(str_url):
			return ""
		return str_url

	def get_url(self) -> str:
		if len(self.scraped_urls["wikivoyage"]) != 0:
			url2scrape = self.scraped_urls["wikivoyage"].popleft()
			try:
				del self.scraped_urls["wikivoyage_hash"][url2scrape]
			except:
				pass
			self.scraped_urls["visited"][url2scrape] = True
			return url2scrape
		elif len(self.scraped_urls["other"]) != 0:
			url2scrape = self.scraped_urls["other"].popleft()
			try:
				del self.scraped_urls["other_hash"][url2scrape]
			except:
				pass
			self.scraped_urls["visited"][url2scrape] = True
			return url2scrape
		else:
			return ""

	def add_url(self, url: str) -> None:
		if self.scraped_urls["visited"].get(url) is True:
			return
		if self.scraped_urls["wikivoyage_hash"].get(url) is True:
			return
		if self.scraped_urls["other_hash"].get(url) is True:
			return
		if url.find("wikivoyage.org") != -1:
			self.scraped_urls["wikivoyage"].append(url)
		else:
			self.scraped_urls["other"].append(url)

	def save_state(self, file_name="history/link_queue.json") -> None:
		with open(file_name, "w", encoding='utf-8') as json_file:
			self.scraped_urls["wikivoyage"] = list(self.scraped_urls["wikivoyage"])
			self.scraped_urls["other"] = list(self.scraped_urls["other"])
			json.dump(self.scraped_urls, json_file, ensure_ascii=False)

	def run_crawler(self) -> None:
		global exit_program
		while True:
			if exit_program:
				self.save_state()
				return
			web_url = self.get_url()
			if web_url == "":
				break
			web_url = urllib.parse.quote(web_url, "<>=/:!")
			try:
				time.sleep(float("1." + str(random.randrange(0, 100))))
				req = urllib.request.Request(web_url, headers=self.req_headers)
				content = urllib.request.urlopen(req, timeout=5).read().decode('utf-8')
			except:
				continue
			output_file = f"data/{hashlib.md5(web_url.encode('utf-8')).hexdigest()}.txt"
			with open(output_file, "w", encoding="utf-8") as file:
				file.write(web_url + '\n')
				file.write(content)
			urls = re.findall(self.url_regex, content)
			relative_urls = re.findall(self.relative_url_regex, content)
			for page_url in relative_urls:
				formatted_url = self.rel_url_format(web_url, page_url)
				if len(formatted_url) != 0:
					self.add_url(formatted_url)
			for page_url in urls:
				formatted_url = self.url_format(page_url)
				if len(formatted_url) != 0:
					self.add_url(formatted_url)
		self.save_state()


print("Running...")

crawler_interrupter = CrawlerInterrupter(daemon=True)
crawler_interrupter.start()

crawler = Crawler()
crawler.run_crawler()

print("Done!")
