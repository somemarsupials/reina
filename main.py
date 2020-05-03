import asyncio
import aiohttp
import csv
from collections import Counter
import requests
from lxml import etree
import nltk

DOMAIN = "https://community.shopify.com"
PATH = "/c/forums/searchpage/tab/message"

def get_comment_list_content(number):
    print("FETCHING PAGE #{}...".format(number))
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
    
    return response.text


def get_comment_urls_from_source(source):
    tree = etree.fromstring(source, etree.HTMLParser())
    
    xpath_query = [
        "//a[@class='page-link lia-link-navigation lia-custom-event']",
        "/@href"
        ]

    return tree.xpath("".join(xpath_query))


def get_comment_text_from_source(source):
    tree = etree.fromstring(source, etree.HTMLParser())

    xpath_query = [
        "//div[@class='lia-component-topic-message']",
        "//div[@class='lia-message-body-content']",
        "/p/text()"
        ]

    return "".join(tree.xpath("".join(xpath_query)))


async def async_fetch_comment(session, url):
    async with session.get(DOMAIN + url) as response:
        print("FETCHING COMMENT ID={}".format(url.split("?")[0].split("/")[-1]))
        return await response.text()


async def async_fetch_comments(comment_urls):
    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.ensure_future(async_fetch_comment(session, url))
            for url in comment_urls
            ]

        return await asyncio.gather(*tasks)


def fetch_all_comments():
    comments = []
    page_number = 1
    loop = asyncio.get_event_loop()

    while True:
        list_source = get_comment_list_content(page_number)
        comment_urls = get_comment_urls_from_source(list_source)

        if not comment_urls:
            print("FOUND EMPTY PAGE")
            break


        comment_sources = loop.run_until_complete(
            async_fetch_comments(comment_urls)
            )

        for source in comment_sources:
            comments.append(get_comment_text_from_source(source))

        page_number += 1

    return comments


def comments_to_word_counts(comments):
    # need to download tokenizer resources!
    nltk.download("punkt")

    raw_words = [
        word.lower() for c in comments
        for word in nltk.tokenize.word_tokenize(c)
        ]

    return Counter(filter(str.isalpha, raw_words))


def counts_to_csv(counts):
    with open("./word_counts.csv", "w") as destination:
        writer = csv.writer(destination)
        writer.writerow(["Word", "Count"])

        for pair in counts.most_common():
            writer.writerow(pair)


counts = comments_to_word_counts(fetch_all_comments())
counts_to_csv(counts)
