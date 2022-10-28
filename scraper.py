import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import lxml.html as lh
import copy

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


domains = {"ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu", "today.uci.edu"}

word_freq = dict()
max_word_url = (None, 0)
crawled_urls = set()

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
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
    global word_freq, max_word_url, crawled_urls

    urls = []
    try:
        if resp.status == 200:
            charset = resp.raw_response.encoding  # response content encoding
            string = resp.raw_response.content.decode(charset)

            soup = BeautifulSoup(string, "html.parser")
            # print(len(set(soup.find_all("a", href=True))))
            is_html = bool(soup.find())
            if is_html:
                # extract urls from the response page
                doc: lh.HtmlElement = lh.fromstring(string, resp.url)
                doc.make_links_absolute()
                for _, _, link, _ in doc.iterlinks():
                    # print(is_valid(link), link, urlparse(link).netloc)
                    urls.append(urlparse(link)._replace(fragment="").geturl())

                assert resp.url not in crawled_urls, f"{resp.url} is already visited"
                crawled_urls.add(resp.url)

                # token statistics
                text = soup.get_text()
                words = tokenize(text)
                freq = compute_word_frequencies(words)
                word_num = len(words)
                if word_num > max_word_url[1]:
                    max_word_url = (resp.url, word_num)
                word_freq = add_word_freqs(word_freq, freq)
                print(max_word_url)

        else:
            print(f"error {resp.status}: {resp.error}, {resp.url}")
    except:
        print(f"error {resp.status}: {resp.error}, {resp.url}, {resp.raw_response.apparent_encoding}, {resp.raw_response.__dict__}")
        raise
    # assert False
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
        # check if url belongs to today.uci.edu/department/information_computer_sciences/
        elif re.match(".*" + "today.uci.edu" + "$", parsed.hostname) is not None:
            if "department/information_computer_sciences" not in parsed.path:
                return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
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


if __name__ == "__main__":
    soup = BeautifulSoup("?ahuihuia>", "html.parser")
    print(soup)