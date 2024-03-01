# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sys
import yaml
import psycopg2
from re import findall
from datetime import datetime
from urllib.parse import urlparse
from itemadapter import ItemAdapter
from bookscraper.settings import CONFIG_FILENAME

class PostgresPipeline:
    def __init__(self) -> None:
        self.config = self.read_postgres_config()
        try:
            self.conn  = psycopg2.connect(
                            f"dbname='{self.config['pg_instance']['dbname']}' \
                              host='{self.config['pg_instance']['host']}' \
                              port={self.config['pg_instance']['port']} \
                              user='{self.config['pg_instance']['username']}' \
                              password='{self.config['pg_instance']['password']}'")
        except Exception as e:
            print("ERROR: Unexpected error: Could not connect to PostgreSQL instance.")
            print(e)
            sys.exit()
        print("SUCCESS: Connection to PostgreSQL instance succeeded")
        self.cur = self.conn.cursor()
        self.start_time = datetime.now()
        self.newline = "\n"
        self.create_required_schemas(schema_names=['stg', 'dwh'])
        self.create_required_tables(table_names={'stg' : ['books'], 'dwh' : ['books']})
        self.numerical_field = ['number_of_reviews', 'price' ,'price__excl__tax_', 'price__incl__tax_', 'rating', 'tax']
    
    def create_required_schemas(self, schema_names: str | list[str]):
        if type(schema_names) == list:
            for schema_name in schema_names:
                self.cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {schema_name};                 
                """)
                self.conn.commit()
        else:
            self.cur.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {schema_names};                 
            """)
            self.conn.commit()
    
    def create_required_tables(self, table_names: dict):
        for schema, tables in table_names.items():
            for table in tables:
                self.cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    {f', {self.newline}'.join([f"{field_name} {datatype}" for field_name, datatype in self.config['tables_config'][schema][table].items()])}
                );                 
                """)
                self.conn.commit()
    
    def read_postgres_config(self) -> dict:
        with open(CONFIG_FILENAME, "r") as stream:
            try:
                config = yaml.safe_load(stream=stream)
            except yaml.YAMLError as yamlErr:
                print(yamlErr)
        return config
    
    def process_item(self, item, spider):
        schema, table = 'stg', 'books'
        config = self.config['tables_config'][schema][table]
        query = f"""
        INSERT INTO {schema}.{table} (
            {', '.join([field for field in config.keys()])}
        )
        VALUES (
            {f"'{item['url']}'"},
            {f"'{item['name']}'"},
            {f"'{item['img_url']}'"},
            {item['price']},
            {f"'{item['category']}'"},
            {item['rating']},
            {f"'{item['product_description']}'"},
            {f"'{item['upc']}'"},
            {f"'{item['product_type']}'"},
            {item['price__excl__tax_']},
            {item['price__incl__tax_']},
            {item['tax']},
            {item['availability']},
            {item['number_of_reviews']},
            {f"'{datetime.now()}'"}
        )
        """
        self.cur.execute(query)
        self.conn.commit()
        
        return item
        
    def close_spider(self, spider):
        schema, table = 'dwh', 'books'
        config = self.config['merges'][schema][table]
        print(f"Config: {config}")
        update_query_col = ', '.join([f'{field} = src.{field}' for field in self.config['tables_config'][config['tgt'].split('.')[0]][config['tgt'].split('.')[1]].keys() if field != 'id'])
        update_query = f"UPDATE SET {update_query_col}"
        insert_query = f"""
        INSERT (
            {', '.join([field for field in self.config['tables_config'][config['src'].split('.')[0]][config['src'].split('.')[1]].keys() if field != 'id'])}
        )
        VALUES (
            {', '.join([f'src.{field}' for field in self.config['tables_config'][config['src'].split('.')[0]][config['src'].split('.')[1]].keys()])}
        )
        """
        query = f"""
        MERGE INTO {config['tgt']} tgt
        USING (SELECT * FROM {config['src']} WHERE updated_at > '{self.start_time}') src
        ON {' AND '.join([f"src.{field} = tgt.{field}" for field in config['merge_on'].split(',')])}
        WHEN MATCHED THEN 
            {update_query}
        WHEN NOT MATCHED THEN
            {insert_query}
        """
        self.cur.execute(query)
        self.conn.commit()
        print("Merged into DWH! Close connection...")
        self.cur.close()
        self.conn.close()
        
class BookscraperPipeline:
    rating_map = {
        'zero' : 0,
        'one' : 1,
        'two' : 2,
        'three' : 3,
        'four' : 4,
        'five' : 5
    }
    
    def extract_numbers(self, input_string):
        # Define a regular expression pattern to match numbers
        number_pattern = r'\d+\.\d+|\d+'
        
        # Use re.findall to find all occurrences of the pattern in the input string
        numbers = findall(number_pattern, input_string)
        
        # Convert the list of strings to a list of floats
        numbers = [float(num) for num in numbers]
    
        return numbers
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item=item)
        
        urls = ["url", "img_url"]
        for field in urls:
            adapter[field] = urlparse(adapter.get(field)).geturl()
            
        price_fields = ["price", "price__excl__tax_", "price__incl__tax_", "tax", "availability", "number_of_reviews"]
        for price_field in price_fields:
            adapter[price_field] = self.extract_numbers(adapter.get(price_field))[0]
        
        adapter['rating'] = self.rating_map[adapter.get('rating').lower().replace('star-rating', '').strip()]
        lowercase_fields = ["category", "product_type"]
        for lowercase_field in lowercase_fields:
            adapter[lowercase_field] = adapter.get(lowercase_field).lower().strip()
            
        remove_quotes_fieldnames = ["name", "category", "product_description", "product_type"]
        for field in remove_quotes_fieldnames:
            adapter[field] = adapter.get(field).replace("'", '')
        return item
