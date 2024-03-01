# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BookItem(scrapy.Item):
    url = scrapy.Field()
    name = scrapy.Field()
    img_url = scrapy.Field()
    price = scrapy.Field()
    category = scrapy.Field()
    rating = scrapy.Field()
    product_description = scrapy.Field()
    upc = scrapy.Field()
    product_type = scrapy.Field()
    price__excl__tax_ = scrapy.Field()
    price__incl__tax_ = scrapy.Field()
    tax = scrapy.Field()
    availability = scrapy.Field()
    number_of_reviews = scrapy.Field()
