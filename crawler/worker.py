from threading import Thread
import copy
import pickle
from urllib.parse import urlparse

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    # default English stopwords list from https://www.ranks.nl/stopwords
    stopwords = {'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'}
    def __init__(self, worker_id, config, frontier, simhash, restart):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.worker_id = worker_id
        self.config = config
        self.frontier = frontier
        self.simhash = simhash
        self.restart = restart
        self.pickle_file = f"stats_{self.worker_id}.pickle"
        self.report_file = f"report_{self.worker_id}.txt"
        self.pickle_init()
        
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests from scraper.py"
        super().__init__(daemon=True)
    
    def pickle_init(self):
        if self.restart:
            # wipes out all content if the file already exists.
            mode = 'wb'
        else:
            # if don't restart, exclusive creation will raise FileExistsError.
            # Ignore the FileExistsError since we want to continue to use the data in that file.
            mode = "xb"
        try:
            with open(self.pickle_file, mode) as file:
                crawled_urls, (max_url, max_word_num), word_freq, ics_subdomains = set(), (None, 0), dict(), set()
                pickle.dump((crawled_urls, (max_url, max_word_num), word_freq, ics_subdomains), file)
        except FileExistsError:
            pass

    def update_stats(self, url, word_freq, token_num):
        domains = [".ics.uci.edu"]
        with open(self.pickle_file, 'rb') as file_r:
            crawled_urls, (max_url, max_word_num), total_word_freq, ics_subdomains = pickle.load(file_r)
            print(f"crawled {len(crawled_urls)} pages. Has {token_num} words. Max so far has {max_word_num}: {max_url}\n")
            
            assert url not in crawled_urls, f"{url} is already crawled"
            crawled_urls.add(url)

            if token_num > max_word_num:
                max_url = url
                max_word_num = token_num
            
            total_word_freq = self.add_word_freqs(total_word_freq, word_freq)

            hostname = urlparse(url).hostname
            if any(d in hostname for d in domains):
                ics_subdomains.add(hostname)
            

        with open(self.pickle_file, 'wb') as file_w:
            pickle.dump((crawled_urls, (max_url, max_word_num), total_word_freq, ics_subdomains), file_w)

    def report_stats(self):
        with open(self.pickle_file, 'rb') as file_r:
            crawled_urls, (max_url, max_word_num), total_word_freq, ics_subdomains = pickle.load(file_r)
        
        msg = ""
    
        # unique pages
        msg += f"visited {len(crawled_urls)} unique pages.\n\n"

        # top 50 words
        topk_words = []
        k = 50
        for word, freq in sorted(total_word_freq.items(), key=lambda item: item[1], reverse=True):
            if word not in Worker.stopwords:
                topk_words.append((word, freq))
                if len(topk_words) >= k:
                    break
        for word, freq in topk_words:
            msg += f"{word} -> {freq}\n"
        msg += "\n"

        # url with the most words
        msg += f"{max_url} has the most words.\nIt has {max_word_num} words in the page.\n\n"

        # list of alphabetically ordered subdomains of ics.uci.edu
        msg += "ics subdomains:\n"
        for domain in sorted(ics_subdomains):
            msg += domain + "\n"
        msg += "\n"

        # write report to disk
        with open(self.report_file, 'w') as file:
            file.write(msg)

    def clean_up(self):
        try:
            self.frontier.save.close()
        except ValueError:
            # closing an already closed shelve file raises ValueError
            # if the file has already been closed before, do nothing more
            pass
        
        if self.simhash.save is not None:
            try:
                self.simhash.save.close()
            except ValueError:
                pass
    
    @staticmethod
    def add_word_freqs(freq_count1: dict, freq_count2: dict):
        freq_count1 = copy.copy(freq_count1)
        freq_count2 = copy.copy(freq_count2)

        # make sure freq_count1 is the larger dict
        if len(freq_count1) < len(freq_count2):
            temp = freq_count1
            freq_count1 = freq_count2
            freq_count2 = temp

        for token in freq_count2.keys():
            freq_count1[token] = freq_count1.get(token, 0) + freq_count2[token]
        return freq_count1
        # return {token: freq_count1.get(token, 0) + freq_count2.get(token, 0) for token in all_tokens}
        
    def run(self):
        while True:
            try:
                tbd_url = self.frontier.get_tbd_url()
                if not tbd_url:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                scraped_urls = scraper.scraper(self, tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            except KeyboardInterrupt:
                raise
            except Exception as err:
                print("Error occured", time.time())
                print(err)
                print(tbd_url)
                print()
            finally:
                try:
                    self.frontier.mark_url_complete(tbd_url)
                    time.sleep(self.config.time_delay)
                except:
                    pass
        
        self.report_stats()
