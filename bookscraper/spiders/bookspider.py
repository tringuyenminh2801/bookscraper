import scrapy
from bookscraper.items import BookItem

class BookspiderSpider(scrapy.Spider):
    name = "bookspider"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com"]
    
    def parse(self, response):
        books = response.css('article.product_pod')
        
        for book in books:
            book_detail = book.css('h3 a::attr(href)').get()
            book_detail_url = f"https://books.toscrape.com/{book_detail if 'catalogue' in book_detail else f'catalogue/{book_detail}'}"
            
            yield response.follow(book_detail_url, callback=self.parse_book_detail)
        
        next_page = response.css('li.next a::attr(href)').get()
        print(f"--------------- \n Next page: {next_page}\n -------------------")
        if next_page is not None:
            
            next_page_url = f"https://books.toscrape.com/{next_page if 'catalogue' in next_page else f'catalogue/{next_page}'}"
            
            yield response.follow(next_page_url, callback=self.parse)
            
    def parse_book_detail(self, response):
        product_information = {
            "".join([c if c.isalnum() else '_' for c in table_row.css("th::text").get().lower()]) : table_row.css("td::text").get() for table_row in response.css('table tr')
        }
        book = BookItem(**{
            'url' : response.url,
            'name' : response.xpath("//p[@class='price_color']/preceding-sibling::h1/text()").get(),
            'img_url' : f"https://books.toscrape.com/{response.css('div.thumbnail img::attr(src)').get().replace('..', '')}",
            'price' : response.xpath("//p[@class='price_color']/text()").get(),
            'category' : response.xpath("//ul[@class='breadcrumb']/li[@class='active']/preceding-sibling::li[1]/a/text()").get(),
            'rating' : response.css("p.star-rating::attr(class)").get(),
            'product_description' : response.xpath("//div[@id='product_description']/following-sibling::p/text()").get(),
            **product_information
        })
        yield book
        