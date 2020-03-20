import scrapy
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor,defer
from scrapy.utils.log import configure_logging



urls = []
article_links = []
'''
for i in range(104):
    url = "https://www.hindawi.com/search/all/nanoparticle/and/journal/journal+of+nanomaterials/{}/".format(i+1)
    urls.append(url)

for i in range(182):
    url = "https://www.hindawi.com/search/all/nanomaterial/and/journal/journal+of+nanomaterials/{}/".format(i+1)
    urls.append(url)
'''

for i in range(78):
    url = "https://www.mdpi.com/search?journal=nanomaterials&page_no={}".format(i + 1)
    urls.append(url)


class SageArticleLinkSpider(scrapy.Spider):
    name = "quotes1"
    start_urls = urls

    def parse(self, response):
        for quote in response.css('a[data-item-name="click-article-title"]::attr(href)').getall():
            article_links.append("https://journals.sagepub.com{}".format(quote))
        with open('sage_article_links.txt', 'wt') as fp:
            fp.write('\n'.join(article_links))



class SageArticleAbstractSpider(scrapy.Spider):
    name = "quotes2"
    start_urls = article_links

    def parse(self, response):
        title = response.css('div[class="art_title"] a::text').get()
        title = ' '.join(title.split())
        abstract = response.css('div[class="abstractSection abstractInFull"] p').get()
        abstract = abstract.split('<p>')[1].split('</p>')[0]
        abstract = abstract.replace('<sub>', '')
        abstract = abstract.replace('</sub>', '')
        abstract = abstract.replace('<sup>', '')
        abstract = abstract.replace('</sup>', '')
        abstract = abstract.replace('<i>', '')
        abstract = abstract.replace('</i>', '')
        abstract = abstract.replace('<b>', '')
        abstract = abstract.replace('</b>', '')
        abstract = abstract.replace('<span class="smallcaps smallerCapital">', '')
        abstract = abstract.replace('</span>', '')
        
        
        #abstract = re.sub(r'\s', '', abstract)
        with open('sage_title_abstracts.txt', 'a') as fp:
            fp.write(title + '. ' +abstract + '\n')



#configure_logging()
runner = CrawlerRunner()

@defer.inlineCallbacks
def crawl():
    yield runner.crawl(SageArticleLinkSpider)
    print(len(set(article_links)))
    yield runner.crawl(SageArticleAbstractSpider)
    reactor.stop()

crawl()
reactor.run() # the script will block here until the last crawl call is finished
