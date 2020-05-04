import asyncio
import aiohttp
from collections import Counter
import csv
from lxml import etree
import nltk

DOMAIN = "https://community.shopify.com"
PATH = "/c/forums/searchpage/tab/message"


class RateLimitedRequests:
    """Makes concurrent HTTP requests. Makes up to a fixed number of requests
    at a time.
    """

    def __init__(self, semaphore):
        self.semaphore = semaphore

    @classmethod
    def with_concurrency(cls, number):
        """Return a client that makes up to `number` requests at a time""" 
        return cls(asyncio.Semaphore(number))

    async def get(self, url, **kwargs):
        """Make an asynchronous HTTP GET request to `url`"""

        async with self.semaphore:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, **kwargs) as response:
                    print("fetch {}".format(url.replace(DOMAIN, "")))

                    return etree.fromstring(
                        await response.text(),
                        etree.HTMLParser()
                        )


def get_comment_urls_from_source(tree):
    """Extract URLs to individual comments out of a page of comments"""

    return tree.xpath(
        "".join([
            "//a[@class='page-link lia-link-navigation lia-custom-event']",
            "/@href"
            ])
        )


def get_comment_text_from_source(tree):
    """Extract comment text from a comment page"""

    comment_fragments = tree.xpath(
        "".join([
            "//div[@class='lia-component-topic-message']",
            "//div[@class='lia-message-body-content']",
            "/p/text()"
            ])
        )

    return "".join(comment_fragments)


async def fetch_comments_for_page(client, number):
    """Get comment text for all comments on a page"""
    print("fetching page #{}".format(number))

    comment_list_source = await client.get(DOMAIN + PATH, params={
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

    comment_sources = await asyncio.gather(*[
        asyncio.ensure_future(client.get(DOMAIN + comment_url))
        for comment_url in get_comment_urls_from_source(comment_list_source)
        ])

    return [get_comment_text_from_source(s) for s in comment_sources]


async def fetch_all_comments(client, page_number=1, comments=[]):
    """Get comment text for all comment pages"""
    page_comments = await fetch_comments_for_page(client, page_number)

    if not page_comments:
        return comments

    return await fetch_all_comments(
        client,
        page_number + 1,
        [*comments, *page_comments]
        )


def comments_to_word_counts(comments):
    """Convert a comment into a word count"""

    # need to download tokenizer resources!
    nltk.download("punkt")

    raw_words = [
        word.lower() for c in comments
        for word in nltk.tokenize.word_tokenize(c)
        ]

    return Counter(filter(str.isalpha, raw_words))


def counts_to_csv(counts):
    """Persist word counts as a CSV file"""

    with open("./word_counts.csv", "w") as destination:
        writer = csv.writer(destination)
        writer.writerow(["Word", "Count"])

        for pair in counts.most_common():
            writer.writerow(pair)


if __name__ == "__main__":
    client = RateLimitedRequests.with_concurrency(10)
    comments = asyncio.get_event_loop().run_until_complete(
        fetch_all_comments(client)
        )

    counts = comments_to_word_counts(comments)
    counts_to_csv(counts)
