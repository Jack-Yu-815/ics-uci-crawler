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

# print([(a, l) for _, a, l, _ in lh.iterlinks(doc)])

import numpy as np
from crawler import SimHash
from configparser import ConfigParser
from argparse import ArgumentParser
from utils.config import Config

parser = ArgumentParser()
parser.add_argument("--restart", action="store_true", default=False)
parser.add_argument("--config_file", type=str, default="config-simhash.ini")
args = parser.parse_args()

cparser = ConfigParser()
cparser.read(args.config_file)
config = Config(cparser)

hasher = SimHash(config, args.restart)
V = hasher._compute_simhash({"worker": 1, "work":1, "works": 3})
hasher.store_simhash("www.test_url.com", {"worker": 1, "work":1, "works": 3})
from utils import get_urlhash
print((hasher.save[get_urlhash("www.test_url.com")] == V).mean())
print(V)


s1 = "this is string one which doesn't have much meaning at all"
s2 = "this is anothing string that differs from the first one and should have low similarity score"
s3 = "this are strings, the ones which doesn't have much meaning at all"
wf1 = compute_word_frequencies(tokenize(s1))
wf2 = compute_word_frequencies(tokenize(s2))
wf3 = compute_word_frequencies(tokenize(s3))


hasher.store_simhash("www.test_url1.com", wf1)
# assert (hasher._compute_simhash(wf1) == hasher._compute_simhash(wf3)).mean() == 1
# assert (hasher.save[get_urlhash("www.test_url.com")] == hasher._compute_simhash(wf1)).mean() == 1, (hasher.save[get_urlhash("www.test_url.com")] == hasher._compute_simhash(wf1)).mean()


print(hasher.max_similarity(wf3))
