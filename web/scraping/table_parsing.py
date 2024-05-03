import re
import pandas as pd
from scrapy.selector import Selector
from typing import List

def parse_row(row:Selector) -> List[str]:
    '''
    Parses a html row into a list of individual elements
    '''
    cells = row.xpath('.//th | .//td')
    row_data = []
    
    for cell in cells:
        cell_text = cell.xpath('normalize-space(.)').get()
        cell_text = re.sub(r'<.*?>', ' ', cell_text)  # Remove remaining HTML tags
        # if there are br tags, there will be some binary characters
        cell_text = cell_text.replace('\xa0', '')  # Remove \xa0 characters
        row_data.append(cell_text)
    
    
    return row_data

def parse_table_as_df(table_sel:Selector,header:bool=True) -> pd.DataFrame:
    '''
    Parses a html table and returns it as a Pandas DataFrame
    '''
    # extract rows
    rows = table_sel.xpath('./tbody//tr')
    
    # parse header and the remaining rows
    columns = None
    start_row = 0
    if header:
        columns = parse_row(rows[0])
        start_row += 1
        
    table_data = [parse_row(row) for row in rows[start_row:]]
    
    # return data frame
    return pd.DataFrame(table_data,columns=columns)