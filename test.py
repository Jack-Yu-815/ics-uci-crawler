import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import lxml.html as lh
import copy
from crawler.worker import Worker
from hashlib import blake2b

from scraper import *
# soup = BeautifulSoup("?ahuihuia>", "html.parser")
doc = lh.document_fromstring("https://baidu.com/info")

urls = [
    "hello this is https://www.i-cs.uci.go-92ogle.com/uci/ics/advising?q1=v1&q-2=v-2#section1",
    "hello ics.uci.edu#resources is the destination of the page\n do you agree? if not, you can visit google.com/info.html",
    "this is /resource1?q1=v1&q2=v2&w-3=v-3#sec_tion1"
]

for url in urls:
    for link in txt_to_urls(url, fragments=False):
        print(link)

import pickle

with open("stats_0.pickle", 'rb') as file:
    crawled_urls, (max_url, max_word_num), total_word_freq, ics_subdomains = pickle.load(file)
print(len(crawled_urls), "urls crawled")
print("subdomains:")
for d in sorted(ics_subdomains):
    print(d, "->", sum(1 for page in crawled_urls if d in urlparse(page).hostname))