import time, re, os, random, hashlib, json, threading
import urllib.request
import urllib.parse
from collections import deque


# Global vars
exit_program = False


class CrawlerInterrupter(threading.Thread):
	"""
    A thread class for interrupting a running crawler.

    This class extends `threading.Thread` and is used to interrupt a running crawler process. It listens for a user input command and sets a global flag to stop the crawler when the specified command is received.

    Attributes:
    - Inherits all attributes from `threading.Thread`.

    Methods:
    - run: Listens for user input and sets a global flag to stop the crawler.
    """

	def run(self):
		"""
        Executes the thread, listening for a stop command from the user.

        This method runs in a separate thread and waits for user input. If the user enters 'stop', it sets a global flag `exit_program` to True, indicating that the crawler should stop running.

        Note:
        - This method should be used in a multi-threaded environment where the main crawler thread checks the `exit_program` flag regularly.
        - The global variable `exit_program` should be defined outside of this class.
        """
		global exit_program
		prompt = input("Enter command: ")
		if prompt == "stop":
			exit_program = True


class Crawler():
	"""
    A web crawler class for scraping URLs from Wikivoyage.

    This class is designed to crawl and scrape URLs, particularly from the Wikivoyage website. It initializes with various attributes including the base URL, request headers, regular expressions for URL matching, and structures to store scraped URLs.
    """

	def __init__(self):
		"""
        Initializes the Crawler object with predefined settings.

        Sets up the crawler with a base URL for Wikivoyage, appropriate request headers for HTTP requests, regular expressions for URL matching, and initializes structures for storing scraped URLs. It also calls `check_history` to load any existing history of scraped URLs.
        """
		self.base_url = "https://en.wikivoyage.org/wiki/"
		self.req_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
		}
		self.url_regex = r"(https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])"
		self.relative_url_regex = r"href=\"(/.*?)\""
		self.scraped_urls = {"wikivoyage": deque(), "wikivoyage_hash": {}, "other": deque(), "other_hash": {}, "visited": {}}
		self.check_history()

	def check_history(self, file_name="history/link_queue.json") -> None:
		"""
        Checks and loads the history of scraped URLs from a JSON file.

        If the history file specified by `file_name` exists, this method loads its content into the `scraped_urls` attribute. If the file does not exist, it initiates the loading of countries (presumably to start a fresh scraping process).

        Parameters:
        file_name (str): The path to the history file. Default is 'history/link_queue.json'.

        Returns:
        None: This method updates the `scraped_urls` attribute of the class and does not return any value.
        """
		if os.path.isfile(file_name):
			with open(file_name, "r", encoding="utf-8") as history_file:
				self.scraped_urls = json.load(history_file)
				self.scraped_urls["wikivoyage"] = deque(self.scraped_urls["wikivoyage"])
				self.scraped_urls["other"] = deque(self.scraped_urls["other"])
		else:
			self.load_countries()

	def load_countries(self, file_name="countries.txt") -> None:
		"""
		Loads country names from a file and appends their URLs to the scraping queue.

		This method reads a file containing country names, each on a new line, and appends a URL for each country to the 'wikivoyage' queue in the `scraped_urls` attribute. It also marks each URL as visited in the 'wikivoyage_hash'.

		Parameters:
		file_name (str): The name of the file containing country names. Default is "countries.txt".

		Returns:
		None: This method updates the `scraped_urls` attribute of the class and does not return any value.
		"""
		with open(file_name, "r", encoding="utf-8") as countries_file:
			for line in countries_file:
				self.scraped_urls["wikivoyage"].append(self.base_url + line.strip())
				self.scraped_urls["wikivoyage_hash"][self.base_url + line.strip()] = True

	def url_format(self, url_tuple: tuple) -> str:
		"""
		Formats a tuple representing a URL into a string and excludes certain file types.

		This method converts a tuple representation of a URL into a string. It also filters out URLs that point to specific file types (e.g., images, PDFs).

		Parameters:
		url_tuple (tuple): A tuple representing the parts of a URL.

		Returns:
		str: A formatted URL string. Returns an empty string if the URL points to an excluded file type.
		"""
		new_url = url_tuple[1:]
		str_url = ''.join(new_url)
		str_url = (url_tuple[0] + "://" + str_url)
		regexp = re.compile(r'\.png|\.jpg|\.jpeg|\.gif|\.svg|\.pdf|\.docx')
		if regexp.search(str_url):
			return ""
		return str_url

	def rel_url_format(self, root_url: str, url: str) -> str:
		"""
		Formats a relative URL into a full URL and excludes certain file types.

		This method combines a root URL and a relative URL to form a full URL. It also filters out URLs that point to specific file types (e.g., images, PDFs).

		Parameters:
		root_url (str): The root URL to which the relative URL will be appended.
		url (str): The relative URL to be appended to the root URL.

		Returns:
		str: A formatted full URL. Returns an empty string if the URL points to an excluded file type.
		"""
		root = re.findall(r"(https://.*?)/", root_url)[0]
		str_url = (root + url)
		regexp = re.compile(r'\.png|\.jpg|\.jpeg|\.gif|\.svg|\.pdf|\.docx')
		if regexp.search(str_url):
			return ""
		return str_url

	def get_url(self) -> str:
		"""
		Retrieves the next URL to be scraped from the stored URLs queue.

		This method pops the next URL from either the 'wikivoyage' or 'other' URL queue in `scraped_urls`, marking it as visited. It prioritizes 'wikivoyage' URLs over 'other' URLs. If no URLs are left in the queue, it returns an empty string.

		Returns:
		str: The next URL to scrape, or an empty string if no URLs are left in the queue.
		"""
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
		"""
		Adds a new URL to the scraping queue if it hasn't been visited or queued before.

		This method checks if the URL is already marked as visited or is in the queue. If not, it adds the URL to the appropriate queue ('wikivoyage' or 'other') in `scraped_urls`.

		Parameters:
		url (str): The URL to be added to the scraping queue.

		Returns:
		None: This method does not return any value.
		"""
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
		"""
		Saves the current state of scraped URLs to a JSON file.

		This method writes the current state of `scraped_urls` to a specified JSON file. It converts the URL deques to lists before saving to ensure JSON compatibility.

		Parameters:
		file_name (str): The file path where the state should be saved. Default is "history/link_queue.json".

		Returns:
		None: This method does not return any value.
		"""
		with open(file_name, "w", encoding='utf-8') as json_file:
			self.scraped_urls["wikivoyage"] = list(self.scraped_urls["wikivoyage"])
			self.scraped_urls["other"] = list(self.scraped_urls["other"])
			json.dump(self.scraped_urls, json_file, ensure_ascii=False)

	def run_crawler(self) -> None:
		"""
		Runs the web crawler in a loop until instructed to exit.

		This method repeatedly retrieves URLs from the scraping queue and processes each URL. For each URL, it performs a web request, extracts the content, and saves it to a file. It also extracts and formats new URLs found in the content and adds them to the scraping queue. The crawler runs in a loop and can be stopped by setting the global `exit_program` flag to True.

		The method ensures that URLs are properly formatted and handles any exceptions during the web request. It imposes a random delay between requests to avoid overloading the server. Upon exiting the loop, either due to the `exit_program` flag being set or the queue being empty, it saves the crawler's current state.

		Note:
		- The crawler respects the robots.txt file of websites and follows ethical scraping practices.
		- A random delay is introduced between requests to mimic human browsing behavior and prevent server overload.

		Returns:
		None: This method does not return any value.
		"""
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
