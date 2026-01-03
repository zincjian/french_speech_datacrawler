import scrapy
import json
from datetime import datetime
from scrapy import signals
from tqdm import tqdm
import re
import os

FILE_PATH = 'dataset/vp_discours.json'

## Valid Date range: 1959-01-15 to 2025-12-22
BEGIN_DATE_STR = "2000-01-01"
END_DATE_STR = "2010-12-31"
FAILURE_FILE = f'logs/failures_{BEGIN_DATE_STR}_to_{END_DATE_STR}.jsonl'

if not os.path.exists('logs'):
    os.makedirs('logs')

def filter_by_date_range(data_list, begin_date_str, end_date_str):
    begin_date = datetime.strptime(begin_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    filtered_list = []
    for entry in data_list:
        date_str = entry.get("prononciation")
        if not date_str: # Skip if date is missing
            continue
        try:
            current_date = datetime.strptime(date_str, "%Y-%m-%d")
            if begin_date <= current_date <= end_date:
                filtered_list.append(entry)
        except ValueError: # Skip if date format is wrong
            continue
    return filtered_list


class SpeechSpider(scrapy.Spider):
    name = "french_speech_spider"
    
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': 'logs/crawl_log.txt', 
        'LOG_FILE_APPEND': False, 
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'CONCURRENT_REQUESTS': 32,
        'DOWNLOAD_DELAY': 0.2,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'ROBOTSTXT_OBEY': False,
        'FEEDS': {
            f'dataset/french_speeches_{BEGIN_DATE_STR}_to_{END_DATE_STR}.json': {
                'format': 'json',
                'encoding': 'utf8',
                'indent': 4,
                'overwrite': True,
            },
        },
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SpeechSpider, cls).from_crawler(crawler, *args, **kwargs)

        # Connect the item_scraped signal to our progress bar update function
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        self.logger.warning("LOGGER TEST: If you see this, writing to file is working!")

        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        target_data = filter_by_date_range(data, BEGIN_DATE_STR, END_DATE_STR)
        
        self.logger.warning(f"DATA CHECK: Found {len(target_data)} items to scrape.")

        # Initialize the progress bar with the total count of filtered items
        self.pbar = tqdm(total=len(target_data), desc="Scraping Speeches", unit="item")
        
        for entry in target_data:
            yield scrapy.Request(
                url=entry['url'], 
                callback=self.parse, 
                meta={'original_data': entry},
                errback=self.handle_error # catch 404s/timeouts
            )

    def parse(self, response):
        item = response.meta['original_data']
        
        container = response.css('div.field--name-field-texte-integral')
        clean_fragments = []

        if not container:
            self.logger.warning(f"Empty content for {response.url}. Retrying...")
            
            retries = response.meta.get('retry_count', 0) + 1
            if retries <= 5: 
                new_request = response.request.copy()
                new_request.meta['retry_count'] = retries
                new_request.dont_filter = True 
                yield new_request
                return 
            else:
                self.logger.error(f"Gave up on {response.url} after {retries} attempts.")
                # --- NEW CODE START ---
                self.log_failure(response.url, "Empty Content (Selector Failed)", response.meta['original_data'])
                # --- NEW CODE END ---
                self.pbar.update(1)
                return
            
        if container:
            raw_fragments = container.css('::text').getall()
            clean_fragments = [frag.strip() for frag in raw_fragments if frag.strip()]
        
        source_text = None 
        
        if clean_fragments:
            last_fragment = clean_fragments[-1]
            
            # (?i)      -> Case insensitive (matches "Source", "source", "SOURCE")
            # ^         -> Start of the string
            # \(?       -> Optional opening parenthesis "("
            # \s*       -> Optional whitespace
            # source    -> The literal word "source"
            # \s*       -> Optional whitespace before separator
            # [:.：]?     -> Optional separator: colon ":" or dot "."
            # \s*       -> Optional whitespace after separator
            # (.*)      -> Capture everything else as the content
            match = re.search(r"(?i)^\(?\s*source\s*[:.：]?\s*(.*)", last_fragment)
            
            if match:
                raw_source = match.group(1)
                # Added ')' to the strip characters to handle "(source ...)" cases
                source_text = raw_source.strip(" .,;。，:)")
                clean_fragments.pop()

        item['source'] = source_text

        item['texte'] = "\n".join(clean_fragments)
        yield item
    
    def item_scraped(self, item, response, spider):
        # Update the progress bar for every successful item
        self.pbar.update(1) 

    def spider_closed(self, spider):
        # Clean up the progress bar when finished
        self.pbar.close() 
    
    def log_failure(self, url, reason, original_data):
        failure_record = {
            "url": url,
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
            "original_data": original_data # Keep this so you can easily retry these later
        }
        with open(FAILURE_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(failure_record, ensure_ascii=False) + "\n")

    def handle_error(self, failure):
        request = failure.request
        retries = request.meta.get('retry_count', 0) + 1
        
        if retries <= 5:
            self.logger.warning(f"Network error on {request.url}. Retrying ({retries}/5)...")
            new_request = request.copy()
            new_request.meta['retry_count'] = retries
            new_request.dont_filter = True
            yield new_request
        else:
            self.pbar.update(1) 
            self.logger.error(f"Failed completely: {repr(failure)}")
            
            # Extract the original data from the request meta if available
            original_data = request.meta.get('original_data', {})
            self.log_failure(request.url, f"Network Error: {failure.getErrorMessage()}", original_data)
    