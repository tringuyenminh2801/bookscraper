import scrapy


class LolchampionsSpider(scrapy.Spider):
    name = "lolchampions"
    allowed_domains = ["www.op.gg"]
    start_urls = ["https://www.op.gg/champions"]

    def parse(self, response):
        pass
