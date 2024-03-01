FROM python:3.11.0

WORKDIR /app/

COPY bookscraper /app/

RUN    pip install --upgrade pip \
    && pip install pipreqs \
    && pipreqs \
    && pip install -r requirements.txt

ENTRYPOINT [ "scrapy", "crawl", "bookspider"]