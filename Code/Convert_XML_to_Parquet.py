import os
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow.parquet as pq

"""
Pre-reqs: 
`pip install pandas pyarrow`
"""

def extract_data_from_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    data = []
    for row in root.findall('.//ABR'):
        abn = row[0].text
        entity_type_ind=row[1][0].text
        state = row[2][1][0][0].text
        data.append({'ABN': abn, 'EntityType': entity_type_ind, 'State': state})
    return data

def get_xml_row_count(xml_file):
    try:
        with open(xml_file, 'r', encoding='utf-8') as file:
            first_line = file.readline()
            start_index = first_line.find('<RecordCount>') + len('<RecordCount>')
            end_index = first_line.find('</RecordCount>', start_index)
            record_count = first_line[start_index:end_index]
            return int(record_count)
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

def get_parquet_row_count(parquet_file):
    table=pq.read_table(parquet_file)
    return table.num_rows


def xml_to_parquet(xml_file, parquet_file):
    data = extract_data_from_xml(xml_file)
    df = pd.DataFrame(data)
    df.to_parquet(parquet_file, index=False)

for i in range(1,21):
    xml_file = f'../Data/20240320_Public{i:02}.xml'
    parquet_file = f'../Data/20240320_Public{i:02}.parquet' 
    if os.path.exists(xml_file):
        xml_to_parquet(xml_file, parquet_file)
        xml_counts=get_xml_row_count(xml_file)
        parquet_counts=get_parquet_row_count(parquet_file)
        if(xml_counts==parquet_counts):
            print(f'{xml_file} has been converted to {parquet_file}, total_rows:{xml_counts}')
        else:
            print(f'Row counts mismatch for {xml_file} and {parquet_file}, please verify.')
    else:
        print(f'{xml_file} does not exist, skipping... ')
