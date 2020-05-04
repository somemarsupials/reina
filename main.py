import asyncio
import aiohttp
from collections import Counter
import csv
from lxml import etree
import nltk

DOMAIN = "https://community.shopify.com"
PATH = "/c/forums/searchpage/tab/message"


class RateLimitedRequests:
    def __init__(self, semaphore):
        self.semaphore = semaphore

    @classmethod
    def with_concurrency(cls, number):
        return cls(asyncio.Semaphore(number))

    async def get(self, url, **kwargs):
        async with self.semaphore:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, **kwargs) as response:
                    return await response.text()


class CommentListPage:
    def __init__(self, tree):
        self.tree = tree

    @classmethod
    def from_source(cls, source):
        return cls(etree.fromstring(source, etree.HTMLParser()))

    def get_comment_urls(self):
        return self.tree.xpath(
            "".join([
                "//a[@class='page-link lia-link-navigation lia-custom-event']",
                "/@href"
                ])
            )


class CommentPage:
    def __init__(self, tree):
        self.tree = tree

    @classmethod
    def from_source(cls, source):
        return cls(etree.fromstring(source, etree.HTMLParser()))

    def get_comment_content(self):
        comment_fragments = self.tree.xpath(
            "".join([
                "//div[@class='lia-component-topic-message']",
                "//div[@class='lia-message-body-content']",
                "/p/text()"
                ])
            )

        return "".join(comment_fragments)


async def fetch_comment_source(client, url):
    return await client.get(DOMAIN + url)


async def fetch_comments(client, urls):
    tasks = [fetch_comment_source(client, url) for url in urls]

    return map(
        lambda source: CommentPage.from_source(source).get_comment_content(),
        await asyncio.gather(*tasks)
        )


async def fetch_comments_for_page(client, number):
    print("fetching page #{}".format(number))

    source = client.get(DOMAIN + PATH, params={
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

    return await fetch_comments(
        client,
        CommentListPage.from_source(await source).get_comment_urls()
        )


async def fetch_all_comments(client, page_number=1, comments=[]):
    """Get comment text for all comment pages"""
    page_comments = list(await fetch_comments_for_page(client, page_number))

    if not page_comments:
        return comments

    return await fetch_all_comments(
        client,
        page_number + 1,
        [*comments, *page_comments]
        )


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


if __name__ == "__main__":
    client = RateLimitedRequests.with_concurrency(10)
    comments = asyncio.get_event_loop().run_until_complete(
        fetch_all_comments(client)
        )

    counts = comments_to_word_counts(comments)
    counts_to_csv(counts)
