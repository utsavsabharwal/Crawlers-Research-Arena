import copy
import re

from lxml.html import tostring
from lxml.etree import XPath

from veetwo.lib.aggregation.spider.docprocessor_helpers import Normalize, ProcessorContentError
from veetwo.lib.aggregation.spider.model import Review, UrlQueueItem, PageContent
from veetwo.lib.aggregation.spider.processors.dynurl import get_amazon_review_url
from veetwo.lib.aggregation.utils import strip_html as strip_all_html_tags
from veetwo.lib.constants import SOURCE_ID
from veetwo.lib.product_to_asin import get_asin_from_url


REGEXPNAMESPACE = {'re': 'http://exslt.org/regular-expressions'}

BAZAARVOICE_REGEX = re.compile(r'bvrr|bvreview', flags=re.IGNORECASE)
BAZAARVOICE_NEXT_PAGES = re.compile(r"""href="(http://review[^'"]*\?page[^"']*)["']""")


def xpath_results(node, xpath_expr):
    if xpath_expr:
        try:
            finder = XPath(xpath_expr, namespaces=REGEXPNAMESPACE)
            return finder(node)
        except Exception, ex:
            raise Exception((ex, xpath_expr))
    return []


def clear_unwanted_subnodes(node, xpath_exprs):
    for xpath_expr in xpath_exprs:
        results = xpath_results(node, xpath_expr)
        for result in results:
            result.drop_tree()


class ElementNotFoundError(ProcessorContentError):
    pass


class NodeStructure(object):

    def __init__(self, key_name, xpath_expr, result_num=None, children_structures=(), excluded_tags=()):
        self.key_name = key_name
        self.xpath_expr = xpath_expr
        self.result_num = result_num
        self.children_structures = children_structures
        self.excluded_tags = excluded_tags

    def extract_structure_from_node(self, root):
        try:
            return self._extract_structure_from_node(root)
        except IndexError, ex:
            raise ElementNotFoundError((self.key_name, ex))

    def _extract_structure_from_node(self, root):
        # copy the node since we clear subnodes which are still used for other purpose
        root = copy.deepcopy(root)
        results = xpath_results(root, self.xpath_expr)
        for result in results:
            clear_unwanted_subnodes(result, self.excluded_tags)
        if self.result_num is not None:
            results = [results[self.result_num]]
        extracted_results = []
        if not self.children_structures:
            if results:
                convert_node = results if isinstance(results, basestring) else results[0]
                results_text = utf8_encode_node(convert_node)
            else:
                results_text = u''
            return {self.key_name: results_text}
        for result in results:
            extracted_result = {}
            for child_structure in self.children_structures:
                extracted_result.update(child_structure.extract_structure_from_node(result))
            extracted_results.append(extracted_result)
        structure = {self.key_name: extracted_results}
        return structure


def parse_date(date_string):
    date_string = re.sub(r'[\(\)]', '', date_string).lower()
    date_string = date_string.replace('date:', '')
    date_string = ''.join([c for c in date_string if ord(c) < 128])
    return Normalize.parse_date(date_string)
    

def utf8_encode_node(node):
    try:
        result_string = tostring(node, encoding='utf-8')
        # hack: lxml sometimes doesn't handle encodings which don't match the specified encoding:
        return guess_encoding_and_convert_to_utf8(result_string)
    except TypeError:
        #  some values returned from xpath expressions are lxml.etree._ElementStringResult
        return unicode(node).encode('utf-8')


def guess_encoding_and_convert_to_utf8(text):
    encodings = ('utf8', 'latin1')
    for encoding in encodings:
        try:
            return text.decode(encoding).encode('utf-8')
        except UnicodeDecodeError:
            pass
    assert False, 'latin1 should decode arbitrary strings to unicode'



class XPathParser(object):

    def __init__(self, sources_datastore, page_structure, xpath_recognizer):
        self.sources_datastore = sources_datastore
        self.page_structure = page_structure
        self.xpath_recognizer = xpath_recognizer

    def parse(self, document, metadata):
        self.document = document
        if document.dom is None:
            return None
        self.url = document.url
        source_row = self.sources_datastore.find_source_metadata(self.url) or {}
        source_metadata = source_row.get('metadata', {})

        if 'next_page_xpath' in source_metadata:
            next_page_xpath = source_metadata.get('next_page_xpath')
            next_page_node_structure = NodeStructure('next_page', next_page_xpath)
            self.page_structure.children_structures.append(next_page_node_structure)

        if not xpath_results(document.dom, self.xpath_recognizer):
            return None
        doc_structure = self.page_structure.extract_structure_from_node(document.dom)
        review_dicts = doc_structure['page_contents'][0]['reviews']
        reviews = []

        for rd in review_dicts:

            if 'external_id' in rd and hasattr(self, 'parse_external_id'):
                rd['external_id'] = self.parse_external_id(rd['external_id'])
            r = Review(self.url,
                       parse_date(rd['pub_date']),
                       rd['author_name'],
                       self.parse_rating(rd['rating']),
                       rd['summary'],
                       rd['full_review'],
                       external_id=rd.get('external_id'))
            reviews.append(r)
        next_page = doc_structure['page_contents'][0].get('next_page')

        if next_page:
            next_urls = [next_page]
            urls = Normalize.normalize_links(self.url, next_urls)
            url_items = [UrlQueueItem(url) for url in urls if url]
        else:
            url_items = []

        pc = PageContent(reviews=reviews,
                         urls=url_items)

        return pc

    def parse_rating(self, rating_string):
        cleaned_rating_string = strip_all_html_tags(rating_string)
        return float(re.findall(r'([0-9]*\.?[0-9]+)', cleaned_rating_string)[0])


class BazaarVoiceParser(XPathParser):

    def __init__(self, sources_datastore):
        excluded_tags = ['.//*[@class="BVReviewFeedbackDisplay"]',
                         './/*[@class="BVReviewSocialBookmarkingSection"]',
                         './/*[@class="BVReviewSectionTitle"]',
                         './/*[re:test(@class, "photosection", "i")]',
                        ]
        reviews_xpath_expr = """.//*[re:test(@class, "^BVStandaloneReviewSectionReview", "i")]
                                | .//*[contains(@class, "BVRRContentReview")]"""
        author_name = NodeStructure('author_name',
                                    """.//span[re:test(@class, "^BVReviewerNickname", "i")]
                                    |.//span[re:test(@class, "^BVRRNickname(\s|$)", "i")]""")
        full_review = NodeStructure('full_review',
                                    """(.//*[@class="BVcontent" and
                                        not(.//*[@class="BVcontentTotal" or @class="BVContentReviewText"])])
                                    | (.//*[@class="BVcontentTotal" or @class="BVContentReviewText"])
				    | (.//*[contains(@class, "BVRRReviewText")])
                                    | (.//*[@class="BVRRReviewDisplayStyle3Content"])""",
                                    result_num=0)
        #@todo: fix this for bestbuy so the summary doesn't include ?? at the end
        summary = NodeStructure('summary',
                                """.//span[re:test(@class, "BV[Rr]eviewTitle(\s|$)", "i")]
				| (.//*[contains(@class, "BVRRReviewTitleContainer")])
                                |.//span[re:test(@class, "(^|\s)BVRRReviewTitle(\s|$)")]""")
                                

        pub_date = NodeStructure('pub_date',
                                 """(.//span[re:test(@class, "BV[Dd]ateCreated", "i") and
                                                not(boolean(.//*[re:test(@class, "dtreviewed", "i")]))])
					       | (.//*[contains(@class, "BVRRReviewDateContainer")])
                                               | (.//span[re:test(@class, "dtreviewed", "i")]) """)
        rating = NodeStructure('rating',
                               """.//*[re:test(@class, "^BVRRRatingNormalOutOf", "i")] 
				| (.//span[re:test(@class, "^BVratingSummaryFinal", "i")])
                                  | (.//*[re:test(@class, "(^|\s)rating", "i")]//@alt)
                                  | (.//*[re:test(@class, "(^|\s)rating", "i")])
				| .//*[re:test(@class, "^BVRRRatingNormalImage", "i")] """,
                               result_num=-1)

        reviews = NodeStructure('reviews',
                                reviews_xpath_expr,
                                children_structures=[author_name, full_review, pub_date, rating, summary],
                                excluded_tags=excluded_tags)
        next_page = NodeStructure('next_page',
                                  """(.//td[@id="BVReviewPaginationNextLinkCell"]/a/@href)
                                                | (.//a[contains(./text(), "next") or
                                                        contains(@name, "Next")]/@href)""")
        page_contents = NodeStructure('page_contents',
                                      '/*',
                                      children_structures=[reviews, next_page])

        super(BazaarVoiceParser, self).__init__(sources_datastore, page_contents, reviews_xpath_expr)

    def parse(self, document, metadata):
        if not BAZAARVOICE_REGEX.search(document.page_text):
            return None
        page_contents = None
        try:
            page_contents = XPathParser.parse(self, document, metadata)
        except ElementNotFoundError:
            # extraction can fail if it matches some but not all of the bazaarvoice patterns
            pass
        if page_contents is None:
            page_contents = PageContent()
        if not page_contents.reviews:
            hreview_results = HReviewParser(self.sources_datastore).parse(document, metadata)
            page_contents.reviews = hreview_results.reviews if hreview_results else []
        page_contents.urls = page_contents.urls or self.get_next_page()
        if page_contents.reviews or page_contents.urls:
            return page_contents
        return None

    def get_next_page(self):
        if not self.document.url.startswith('http://reviews.'):
            urls = list(set(re.findall(r"""(http://reviews[^"']*/reviews.htm)""", self.document.page_text)))
            if len(urls) == 1:
                urls = Normalize.normalize_links(self.url, urls)
                url_items = [UrlQueueItem(url) for url in urls if url]
                return url_items
        urls = set(BAZAARVOICE_NEXT_PAGES.findall(self.document.page_text))
        url_items = [UrlQueueItem(url) for url in urls if url != self.document.url]
        return url_items


class HReviewParser(XPathParser):

    def __init__(self, sources_datastore):
        reviews_xpath_expr = """.//*[contains(@class, "hreview") and
                                     not(contains(@class, "hreview-aggregate"))]"""
        author_name = NodeStructure('author_name', './/*[contains(@class, "reviewer")]')
        full_review = NodeStructure('full_review', './/*[contains(@class, "description")]')
        summary = NodeStructure('summary', './/*[contains(@class, "summary")]')
        pub_date = NodeStructure('pub_date', './/*[contains(@class, "dtreviewed")]')
        rating = NodeStructure('rating',
                                """(.//*[re:test(@class, "(^|\s)rating", "i")]//@alt)
                                   | (.//*[re:test(@class, "(^|\s)rating", "i")])
				   | (.//*[re:test(@class, "(^|\s)prStars prStarsSmall rating", "i")]//@title)
			      | (.//*[re:test(@class, "(^|\s)review, hreview", "i")]//@alt)
				|  (.//*[re:test(@class, "(^|\s)reviewhead", "i")]//@title)""",
                                result_num=-1)
        '''rating = NodeStructure('rating',"""concat(
                                substring((.//*[re:test(@class, "(^|\s)rating", "i")]//@alt),1, number(string-length(.//*[re:test(@class, "(^|\s)rating", "i")]//@alt)>0)*string-length(.//*[re:test(@class, "(^|\s)rating", "i")]//@alt)),
                                substring(normalize-space(.//*[re:test(@class, "(^|\s)rating", "i")]/text()),1, number(string-length(.//*[re:test(@class, "(^|\s)rating", "i")]//@alt)=0 and string-length(normalize-space(.//*[re:test(@class, "(^|\s)rating", "i")]/text()))>0)*string-length(normalize-space(.//*[re:test(@class, "(^|\s)rating", "i")]/text()))),
                                substring((.//*[re:test(@class, "(^|\s)rating", "i")]/@title),1, number(string-length(.//*[re:test(@class, "(^|\s)rating", "i")]//@alt)=0 and string-length(normalize-space(.//*[re:test(@class, "(^|\s)rating", "i")]/text()))=0 and string-length(.//*[re:test(@class, "(^|\s)rating", "i")]/@title)>0)*string-length(.//*[re:test(@class, "(^|\s)rating", "i")]/@title)),
                                substring(count(.//*[@class= "rating"]/*[@class="stars"]/img[@src="/images/star.gif"]),1, number(string-length(.//*[re:test(@class, "(^|\s)rating", "i")]//@alt)=0 and string-length(normalize-space(.//*[re:test(@class, "(^|\s)rating", "i")]/text()))=0 and string-length(.//*[re:test(@class, "(^|\s)rating", "i")]/@title)=0 and count(.//*[@class= "rating"]/*[@class="stars"]/img[@src="/images/star.gif"])>0)*string-length(count(.//*[@class= "rating"]/*[@class="stars"]/img[@src="/images/star.gif"]))))""",
                                result_num=-1)'''
	
        reviews = NodeStructure('reviews', reviews_xpath_expr,
                                children_structures=[author_name, full_review, pub_date, rating, summary])
        page_contents = NodeStructure('page_contents', '/*', children_structures=[reviews])

        super(HReviewParser, self).__init__(sources_datastore, page_contents, reviews_xpath_expr)

    def parse(self, document, metadata):
        page_contents = super(HReviewParser, self).parse(document, metadata)
        if not page_contents:
            return None
        # Some Hreview sites only use the summary field and not the description field
        for review in page_contents.reviews:
            if not review.full_text and review.summary:
                review.full_text = review.summary
                review.summary = ''
        return page_contents


POWERREVIEWS_REGEX = re.compile(r'powerreviews', flags=re.IGNORECASE)
POWERREVIEWS_MAX_RATING_IN_PIXELS = 180.0
POWERREVIEWS_MAX_RATING_IN_STARS = 5.0


class PowerReviewsXPathParser(XPathParser):

    def __init__(self, sources_datastore):
        reviews_xpath_expr = './/div[contains(@class, "prReviewWrap") or contains(@class, "pr-review-wrap")]'
        author_name = NodeStructure('author_name', """.//*[@class="prReviewAuthorName" or @class="pr-review-author-name"]/span/text()""")
        full_review = NodeStructure('full_review', """.//*[@class="prReviewText" or @class="pr-review-text"]//*[@class="prComments" or @class="pr-comments"] | .//*[@class="prReviewText" or @class="pr-review-text"]""", result_num=-1)
        summary = NodeStructure('summary', """.//*[@class="prReviewRatingHeadline" or @class="pr-review-rating-headline"]/text()""")
        #pub_date = NodeStructure('pub_date', """.//span[@class="prReviewAuthorDate" or @class="pr-review-author-date"]/span/text()""")
        pub_date = NodeStructure('pub_date', """concat(substring((.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//text()[string-length(normalize-space(.))>=8 and string-length(translate(., '1234567890/', '')) = 0]), 1 , number(string-length(.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//text()[string-length(normalize-space(.))>=8 and string-length(translate(., '1234567890/', '')) = 0]) > 0) * string-length(.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//text()[string-length(normalize-space(.))>=8 and string-length(translate(., '1234567890/', '')) = 0])), substring(concat(.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//*[@class="pr-date-month"],'/',.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//*[@class="pr-date-day"],'/',.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//*[@class="pr-date-year"]), 1, number(not((string-length(.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//text()[string-length(normalize-space(.))>=8 and string-length(translate(., '1234567890/', '')) = 0]) > 0))) * string-length(concat(.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//*[@class="pr-date-month"],'/',.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//*[@class="pr-date-day"],'/',.//*[contains(@class,"prReviewAuthorDate") or contains(@class,"pr-review-author-date")]//*[@class="pr-date-year"]))))""")
        rating = NodeStructure('rating', """re:replace(.//div[contains(@class, "prStarsSmall") or contains(@class, "prStars") or contains(@class, "pr-stars-small")]/@style, "background-position: 0px -(\\d+)px.*", "", "\\1")""")
        reviews = NodeStructure('reviews', reviews_xpath_expr,
                                children_structures=[author_name,
                                                     full_review,
                                                     pub_date,
                                                     rating,
                                                     summary
                                                    ])
        page_contents = NodeStructure('page_contents', '/*', children_structures=[reviews])
        super(PowerReviewsXPathParser, self).__init__(sources_datastore, page_contents, reviews_xpath_expr)

    def parse(self, document, metadata):
        if not POWERREVIEWS_REGEX.search(document.page_text):
            return None
        page_contents = None
        try:
            page_contents = XPathParser.parse(self, document, metadata)
        except ElementNotFoundError:
            pass
        if page_contents is None:
            page_contents = PageContent()
        if not page_contents.reviews:
            hreview_results = HReviewParser(self.sources_datastore).parse(document, metadata)
            page_contents.reviews = hreview_results.reviews if hreview_results else []
        if page_contents.reviews or page_contents.urls:
            return page_contents
        return None

    def parse_rating(self, rating_string):
        # ratings are displayed by specifying the number of pixels out of
        # 180 px = 5 stars to expose the stars image
        num_pixels = float(rating_string)
        return POWERREVIEWS_MAX_RATING_IN_STARS * num_pixels / POWERREVIEWS_MAX_RATING_IN_PIXELS


class SourceXPathParser(object):

    def __init__(self, sources_datastore):
        self.source_id_to_parser = {SOURCE_ID.AMAZON: AmazonParser(sources_datastore)}
        self.sources_datastore = sources_datastore

    def parse(self, document, metadata):
        source_metadata = self.sources_datastore.find_source_metadata(document.url)
        parser = None
        if source_metadata:
            if source_metadata.id in self.source_id_to_parser:
                parser = self.source_id_to_parser[source_metadata.id]
            elif hasattr(source_metadata.metadata, 'page_content_node_structure'):
                assert source_metadata.metadata.reviews_xpath_expr, \
                       'reviews_xpath_expression not found, url=%s' % document.url
                parser = XPathParser(self.sources_datastore,
                                     source_metadata.metadata.page_content_node_structure,
                                     source_metadata.metadata.reviews_xpath_expr)
        if parser is not None:
            return parser.parse(document, metadata)
        else:
            return None


class AmazonParser(XPathParser):

    def __init__(self, sources_datastore):
        source_metadata = sources_datastore.get_source_by_id(SOURCE_ID.AMAZON)
        page_content_node_structure = source_metadata.metadata.page_content_node_structure
        reviews_xpath_expr = source_metadata.metadata.reviews_xpath_expr

        super(AmazonParser, self).__init__(sources_datastore,
                                           page_content_node_structure,
                                           reviews_xpath_expr)

    def parse(self, document, metadata):
        # TODO add ability to handle multiple formats for a/b tests
        # (ran into during testing)
        source_metadata = self.sources_datastore.find_source_metadata(document.url)
        assert source_metadata and source_metadata.id == SOURCE_ID.AMAZON, \
               'AmazonParser cannot parse non-amazon urls, url=%s' % document.url
        if 'astore.amazon.com' in document.url:
            # ignore astore.amazon.com urls
            return None
        if 'product-reviews' not in document.url:
            asin = get_asin_from_url(document.url)
            assert asin, 'no asin found for url=%s' % document.url
            return PageContent(urls=[UrlQueueItem(get_amazon_review_url(asin))])
        page_contents = super(AmazonParser, self).parse(document, metadata)
        return page_contents or PageContent()

    def parse_external_id(self, external_id):
        matches = re.findall(r'/profile/([^/]*)', external_id)
        if matches:
            return matches[0]
        return external_id
