import os
import requests
from scrapy import Selector
from table_parsing import parse_row,parse_table_as_df
from pathlib import Path

############ SETUP #############
DATA_FOLDER = Path('data/')
URLS = [
    # wikipedia population article
    'https://en.wikipedia.org/wiki/World_population',
    # alcohol mortality summary data from CDC
    'https://www.cdc.gov/nchs/fastats/alcohol.htm'
]

if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

def get_selector_from_url(url:str) -> Selector:
    response = requests.get(url)
    return Selector(text=response.content)

############# CASE STUDY 1 #############
selector = get_selector_from_url(URLS[0])

# select all tables whose class attribute contains "wikitable"
wikitables = selector.xpath('//table[contains(@class,"wikitable")]')
print(f'Number of tables: {len(wikitables)}')

for i,wikitable in enumerate(wikitables):
    try:
        df = parse_table_as_df(wikitable,header=True)
    except Exception as e:
        with open(DATA_FOLDER/f'error_wiki_table_{i}.csv','w') as f:
            caption = wikitable.xpath("./caption/text()").get()
            f.write(f'Table caption: {caption}')
        
        continue

    # save as csv
    df.to_csv(DATA_FOLDER/f'population_wiki_table_{i}.csv',index=False)

############# CASE STUDY 2 #############
selector = get_selector_from_url(URLS[1])
div_sel = selector.xpath('//div[@class="card mb-3" and div[1][text()="Mortality"]]')
list_entries = div_sel.xpath('.//li/text()').getall()

# save list entries as a text file
list_entries_str = '\n'.join(list_entries)
with open(DATA_FOLDER/"cdc_alcohol_mortality.txt",'w') as f:
    f.write(list_entries_str)