import xml.etree.ElementTree as ET
import wget
from zipfile import ZipFile
import os
from collections import Counter
import xml.dom.minidom
import pandas as pd
import boto3
import logging

"""
This program performs web-scrapping on a given XML. It outputs a csv file which has the specified attributes extracted from the XML. The csv file is also uploaded to an AWS S3 bucket.

Note: This project requires preconfigured AWS cli.
"""

# ----------------- Helper Function to Pretty Print tree --------------------
def print_tree():
    """Function to print XML data in a more readable manner."""
    with open(data_file_path, encoding='utf8') as xmldata:
        xml = xml.dom.minidom.parseString(xmldata.read())  # or xml.dom.minidom.parseString(xml_string)
        xml_pretty_str = xml.toprettyxml()

    print(xml_pretty_str)
# ---------------------------------------------------------------------------

# 1. Downloading the xml file.
SOURCE_URL = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'
wget.download(SOURCE_URL, 'source.xml')
logging.info('Source XML file downloaded.')

# 2. From the xml, please parse through to the first download link whose file_type is DLTINS and download the zip.
def download_zip():
    """
    Downloads the zip file from the first download_link of the first doc of type DLTINS.
    """
    source_tree = ET.parse('source.xml')
    source_root = source_tree.getroot()
    
    # 2(a). Getting the first download link whose file_type is DLTINS
    # myroot[1] = result, myroot[1][0] = first child of result, and so on
    for tag in source_root[1][0]:
        if tag.attrib['name'] == 'download_link':
            temp = tag.text
        
        if tag.attrib['name'] == 'file_type' and tag.text == 'DLTINS':
            download_link = temp
            logging.info('Download link of file_type DLTINS found.')
            break

    # 2(b). Downloading the zip         
    ZIP_URL = download_link
    response = wget.download(ZIP_URL, 'zip_data.zip')
    logging.info('Zip file downloaded.')
    
# 3. Extract the xml from the zip.
def extract_zip():
    """
    Extracts the zip file downloaded from the link extracted from download_zip() function. The extracted file will be saved in the newly created data dir.
    """
    with ZipFile('zip_data.zip', 'r') as zip:
        zip.extractall('data')
    logging.info('Zip file extracted and saved to data dir.')
    
    # Assigning data path to a var
    global data_file_name, data_file_path
    data_file_name = os.listdir('./data')[0]
    data_file_path = './data/' + data_file_name

# 4. Convert the contents of the xml into a CSV with the following header.
def xml_to_csv():
    """
    Converts the XML file extracted from the zip file in extract_zip() function to CSV.
    The CSV file will have five headers, namely, Id, FullNm, ClssfctnTp, CmmdtyDerivInd, NtnlCcy, Issr. It will be saved by the name 'data.csv'.
    """
    data_tree = ET.parse(data_file_path)
    data_root = data_tree.getroot()

    all_attribs = []
    # Iterating over each FinInstrm to get the attributes, store them in an array, and then store the array in another array
    for x in data_root[1][0][0]:
        if x.tag.endswith('FinInstrm'):
            Id = x[0][0][0].text # Id
            FullNm = x[0][0][1].text # FullNm
            ClssfctnTp = x[0][0][3].text # ClssfctnTp
            CmmdtyDerivInd = x[0][0][5].text # CmmdtyDerivInd
            NtnlCcy = x[0][0][4].text # NtnlCcy
            Issr = x[0][1].text # Issr
            
            attribs = [Id, FullNm, ClssfctnTp, CmmdtyDerivInd, NtnlCcy, Issr]

            all_attribs.append(attribs)

    # Making a dataframe out of array            
    xml_df = pd.DataFrame(all_attribs, columns = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr'])

    # Saving the dataframe as a csv file.
    xml_df.to_csv('data.csv', index=False)
    logging.info('CSV file created.')

# 5. Storing the csv in an AWS S3 Bucket.
s3 = boto3.client('s3')
def create_bucket(bucket_name):
    """
    Creates an AWS S3 bucket of the specified name.
    Accepts bucket_name in form of string.
    
    Hint: Make the bucket name as unique as possible since, bucket namespace is shared across all the users of AWS.
    """
    s3.create_bucket(Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint':'us-east-2'})
    logging.info(f'Bucket {bucket_name} created.')
    
def upload_to_bucket(bucket_name):
    """
    Uploads the CSV file generated in the function xml_to_csv() to the AWS S3 Bucket. Accepts the bucket_name in form of string where the file needs to be uploaded.
    """
    response = s3.upload_file('data.csv', bucket_name, 'data.csv')
    logging.info(f'File uploaded to the bucket {bucket_name}.')

if __name__=="__main__":
    download_zip()
    extract_zip()
    xml_to_csv()
    
    try:
        create_bucket('steel-eye-task')
    except Exception as e:
        logging.exception("Error occurred while creating the bucket.\nHint: Try changing the name of the bucket. ")
        
    upload_to_bucket('steel-eye-task')