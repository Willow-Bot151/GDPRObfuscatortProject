import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError

random_test_data = {
    'Name' : [
        'Anaya Wong',
        'Walter Fernandez',
        'Amara Hardin',
        'Hassan Mora',
        'Jemma Hill',
        'Isaac Fitzpatrick',
        'Annabella Ahmed',
        'Harry Villalobos',
        'Zoya Diaz',
        'Nathan Simpson',
        'Anastasia Marsh',
        'Bo Burke',
        'Vera Harmon',
        'Roberto McIntosh',
        'Gwen Bell',
        'Emmett Ford',
        'Alexandra Reynolds',
        'Vincent Michael',
        'Aubriella Lang',
        'Wells Walters'],
    'DoB' : [
        '10/07/1920',
        '25/02/1923',
        '15/05/1933',
        '17/09/1941',
        '21/05/1942',
        '26/07/1950',
        '25/01/1955',
        '01/10/1960',
        '16/05/1971',
        '08/06/1972',
        '18/09/1973',
        '05/03/1989',
        '12/06/1992',
        '04/10/1993',
        '14/07/2005',
        '28/11/2005',
        '08/01/2009',
        '30/03/2018',
        '01/07/2023',
        '22/07/2023'],
    'OrderReference' : [
        5203,
        3577,
        3553,
        1926,
        8259,
        2999,
        5069,
        8924,
        6084,
        7637,
        8775,
        5395,
        7091,
        9158,
        4783,
        3147,
        2919,
        3289,
        5827,
        5748] 
}

df = pd.DataFrame.from_dict(random_test_data)

df.to_csv(path_or_buf='temp/test_data_csv')

s3_client = boto3.client('s3')
s3_client.create_bucket(Bucket='testDataBucket')
