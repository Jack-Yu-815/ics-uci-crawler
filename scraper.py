import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import lxml.html as lh
from hashlib import blake2b
import pickle
from crawler.worker import Worker

# How many unique pages did you find? Uniqueness for the purposes of this assignment is ONLY established by the URL,
# but discarding the fragment part. So, for example, http://www.ics.uci.edu#aaa and http://www.ics.uci.edu#bbb are
# the same URL. Even if you implement additional methods for textual similarity detection, please keep considering
# the above definition of unique pages for the purposes of counting the unique pages in this assignment.
#
# What is the longest page in terms of the number of words? (HTML markup doesn’t count as words)
#
# What are the 50 most common words in the entire set of pages crawled under these domains ? (Ignore English stop
# words, which can be found, for example, hereLinks to an external site.) Submit the list of common words ordered by
# frequency.
#
# How many subdomains did you find in the ics.uci.edu domain? Submit the list of subdomains ordered alphabetically
# and the number of unique pages detected in each subdomain. The content of this list should be lines containing
# subdomain, number, for example: vision.ics.uci.edu, 10 (not the actual number here)


domains = {".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu"}

# worker.word_freq = dict()
# max_word_url = (None, 0)
# crawled_urls = set()

def scraper(worker, url, resp):
    links = extract_next_links(worker, url, resp, min_token=130)
    return [link for link in links if is_valid(link)]


def extract_next_links(worker: Worker, url, resp, min_token):
    """

    Parameters
    ----------
    url: str
    resp: utils.response.Response

    Returns
    -------
    list
        A list of scraped url strings.
    """
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    urls = []
    try:
        if resp.status == 200:
            if 'Content-Type' not in resp.raw_response.headers or any(format in resp.raw_response.headers['Content-Type'].lower() for format in ["text/html", "text/plain"]):
                if 'Content-Type' in resp.raw_response.headers:
                    charset = resp.raw_response.encoding  # response content encoding
                else:
                    charset = resp.raw_response.apparent_encoding
                
                if charset is None:
                    charset = 'utf-8'

                # in case the response header returns the wrong charset encoding
                try:
                    text = resp.raw_response.content.decode(charset)
                except UnicodeDecodeError:
                    if resp.raw_response.apparent_encoding is not None:
                        text = resp.raw_response.content.decode(resp.raw_response.apparent_encoding)
                    else:
                        raise

                # if content type is HTML
                if 'Content-Type' not in resp.raw_response.headers or "text/html" in resp.raw_response.headers['Content-Type'].lower():
                    soup = BeautifulSoup(text, "html.parser")
                    # extract urls from the response page
                    doc: lh.HtmlElement = lh.fromstring(resp.raw_response.content, resp.url)
                    doc.make_links_absolute()
                    
                    for _, _, link, _ in doc.iterlinks():
                        urls.append(urlparse(link)._replace(fragment="").geturl())

                    # token statistics
                    text = soup.get_text()

                # if content type is plain text
                elif "text/plain" in resp.raw_response.headers['Content-Type'].lower():
                    text = resp.raw_response.content.decode(charset)
                    urls.extend(txt_to_urls(text, fragments=False))
                
                # otherwise, don't crawl
                else:
                    return []

                # if any of the accepted format, compute token statistics
                tokens = tokenize(text)
                word_freq = compute_word_frequencies(tokens)
                token_num = len(tokens)

                if worker.simhash is not None:
                    max_url, max_sim = worker.simhash.max_similarity(word_freq)
                    print(max_sim, max_url)

                # don't crawl low info page
                if token_num < min_token:
                    return []
                # don't crawl near duplicate page
                elif worker.simhash is not None:
                    if worker.simhash.is_near_duplicate(word_freq, threshold=0.995):
                        return []
                    else:
                        worker.simhash.store_simhash(resp.url, word_freq)
                
                worker.update_stats(resp.url, word_freq, token_num)

        else:
            print(f"error {resp.status}: {resp.error}, {resp.url}")
    
    except UnicodeDecodeError:
        print("UnicodeDecodeError:")
        print(f"error {resp.status}: {resp.error}, {resp.url}, {resp.raw_response.apparent_encoding}, {resp.raw_response.encoding}, {resp.raw_response.headers}")
    except:
        if resp.raw_response:
            print(f"error {resp.status}: {resp.error}, {resp.url}, {resp.raw_response.apparent_encoding}, {resp.raw_response.encoding}, {resp.raw_response.headers}")
        raise
    
    return urls


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        elif all(re.match(".*" + d + "$", parsed.hostname) is None for d in domains):
            return False

        elif all(d not in parsed.hostname for d in domains):
            return False
        elif "swiki.ics.uci.edu" in parsed.hostname:  # a trap
            return False
        elif "archive.ics.uci.edu" in parsed.hostname and "ml/datasets.php" in parsed.path:  # a trap
            return False
        elif "wics.ics.uci.edu" in parsed.hostname and "events" in parsed.path:  # a trap
            return False
        elif "cbcl.ics.uci.edu" in parsed.hostname and "do=diff" in parsed.query:  # a trap
            return False
        # check if url belongs to today.uci.edu/department/information_computer_sciences/
        elif re.match(".*" + "today.uci.edu" + "$", parsed.hostname) is not None:
            if "department/information_computer_sciences" not in parsed.path:
                return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4|webm"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz"
            + r"|r|bib|py|git|pdf)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed, url)

        # there may be bad syntax urls. A url can be visited if hostname
        if parsed.netloc != "":
            raise


# linear time complexity (maybe slower than linear, but faster than n squared).
# It depends on the complexity of the regular expression.
def tokenize(text) -> list:
    word_pattern = re.compile(r"\b\S+\b")
    tokens = []
    words = word_pattern.findall(text)
    for w in words:
        tokens.append(w.lower())

    return tokens


# linear time complexity
def compute_word_frequencies(tokens: []) -> dict:
    count = dict()
    for t in tokens:
        if t not in count:
            count[t] = 1
        else:
            count[t] += 1
    return count


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


# linear time complexity
def print_freq(frequencies):
    for token, count in sorted(frequencies.items(), key=lambda item: item[1], reverse=True):
        print(f"{token} -> {count}")


def txt_to_urls(text, fragments=False):
    # scheme (optional)
    # (?:(?P<scheme>http|https)://)?
    # 
    # hostname (required)
    # (?P<hostname>(?:[\w-]+\.)+[a-z]{2,3})
    # 
    # path (optional)
    # (?P<path>(?:/[\w-]+)+(?:\.[a-z]+)?)?
    # (?P<path>(?:/[\w-]+)+)?
    # 
    # query (optional)
    # (?P<query>\?(?:[\w-]+=[\w-]+(?:&[\w-]+=[\w-]+)*))?
    # 
    # fragment (optional)
    # (?P<fragment>#[\w-]+)?
    ans = []
    abs_url_pattern = re.compile(r"((?:(?P<scheme>http|https)://)?(?P<hostname>(?:[\w-]+\.)+[a-z]{2,3})(?P<path>(?:/[\w-]+)+(?:\.[a-z]+)?)?(?P<query>\?(?:[\w-]+=[\w-]+(?:&[\w-]+=[\w-]+)*))?(?P<fragment>#[\w-]+)?)")
    for text_tuple in abs_url_pattern.findall(text):
        url = text_tuple[0]
        if not fragments:
            url = urlparse(url)._replace(fragment="").geturl()
        ans.append(url)

    return ans

if __name__ == "__main__":
    invalid_urls = ["http://motifmap.ics.uci.edu/videos/SNPer.webm", "http://www.cecs.uci.edu", "http://www.ics.uci.edu/~xhx/publications/nc_vel_tuning.pdf", "https://wics.ics.uci.edu/events/2022-01-20/?ical=1"]
    for url in invalid_urls:
        print(is_valid(url))
