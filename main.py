from collections import Counter
import csv
from lxml import etree
import requests
import string

DOMAIN = "https://community.shopify.com"
PATH = "/c/forums/searchpage/tab/message"

def parse_page_source_into_element_tree(source):
    return etree.fromstring(source, etree.HTMLParser())


def get_comment_urls_from_comment_list_page(source):
    element_tree = parse_page_source_into_element_tree(source)

    xpath_query_parts = [
        # get anchors with class "page-link lia-link-navigation lia-custom-event"
        # anchor == a in HTML
        "//a[@class='page-link lia-link-navigation lia-custom-event']",
        # get href property from matching elements
        "/@href"
        ]

    # use join to combine query parts into a single query string
    return element_tree.xpath("".join(xpath_query_parts))


def get_comment_content_from_comment_page(source):
    element_tree = parse_page_source_into_element_tree(source)

    xpath_query_parts = [
        # get divs with class "lia-component-topic-message"
        "//div[@class='lia-component-topic-message']",
        # get any div within these with class "lia-component-body-content"
        "//div[@class='lia-message-body-content']",
        # get paragraphs within these
        # paragraph == p in HTML
        "/p",
        # get text content of paragraphs
        "/text()"
        ]

    # use join to combine query parts into a single query string
    paragraphs = element_tree.xpath("".join(xpath_query_parts))
    return "".join(paragraphs)


def fetch_comment_from_url(comment_url):
    print("fetching {}".format(comment_url))

    comment_source = requests.get(DOMAIN + comment_url).text
    return get_comment_content_from_comment_page(comment_source)


def fetch_comments_from_urls(comment_urls):
    return [fetch_comment_from_url(url) for url in comment_urls]


def fetch_all_comments_for_page(number):
    print("fetching page #{}".format(number))

    response = requests.get(DOMAIN + PATH, params={
        "filter": "location,dateRangeType",
        "q": "review%20app",
        "noSynonym": "false",
        "advanced": "true",
        "rangeTime": "1y",
        "location": "forum-board:shopify-discussion",
        "sort_by": "score",
        "collapse_discussion": "true",
        "search_type": "thread",
        "search_page_size": "50",
        "page": str(number)
        })

    comment_urls = get_comment_urls_from_comment_list_page(response.text)
    return fetch_comments_from_urls(comment_urls)


def fetch_all_comments(page_num=1, all_comments=[]):
    new_comments = fetch_all_comments_for_page(page_num)

    if not new_comments:
        return all_comments

    # note that we use recursion here!
    # this makes the code cleaner because we don't need a while loop
    return fetch_all_comments(page_num + 1, all_comments + new_comments)


def remove_punctuation_and_lowercase(word):
    # use translate to remove punctuation
    # https://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string
    return word.lower().translate(word.maketrans("", "", string.punctuation))


def comments_to_word_counts(comments):
    # note use of double loop in list comprehension
    raw_words = [word for comment in comments for word in comment.split()]

    processed_words = [
        remove_punctuation_and_lowercase(word)
        for word in raw_words if len(word) > 0 and word.isalpha()
        ]

    return Counter(processed_words)


def counts_to_csv(counts):
    with open("./word_counts.csv", "w") as destination:
        writer = csv.writer(destination)
        writer.writerow(["Word", "Count"])

        for pair in counts.most_common():
            writer.writerow(pair)


if __name__ == "__main__":
    comments = fetch_all_comments()
    counts = comments_to_word_counts(comments)
    counts_to_csv(counts)
