import json
from lib.use_s3fs import getS3Fs
from time import strftime, time, sleep

s3 = getS3Fs()
def process_job(data):
    try:
        job_data = json.loads(data)
        years = job_data['years'].split(';')
        row = job_data['row'].split(';')
        
        country_source = job_data['country_source']
        product_code = job_data['product_code']
        product_name = job_data['product_name']
        unit = job_data['unit']
        category = job_data['category']
        for i, year in enumerate(years):
            result = {
              'link': job_data['link'],
              'domain': 'trademap.org',
              'tags': ['trademap.org', category, product_name],
              'crawling_time': strftime('%Y-%m-%d %H:%M:%S'),
              'crawling_time_epoch': int(time()),
              'path_data_raw': f's3://ai-pipeline-statistics/data/data_raw/trademap/yearly/{category}/{year}/{"_".join([country_source, row[0], product_name, unit]).replace(" ", "-")}.json',
              'path_data_clean': f's3://ai-pipeline-statistics/data/data_clean/trademap/yearly/{category}/{year}/{"_".join([country_source, row[0], product_name, unit]).replace(" ", "-")}.json',
              'country_source': country_source,
              'country_target': row[0],
              'product_name': product_name,
              'product_code': product_code,
              'year': year,
              'category': category,
              'value': row[i + 1],
              'unit': unit
            }
            json.dump(result, s3.open(result['path_data_raw'].replace('s3://', ''), 'w'))
        return f'[Success]: {country_source} {category} to {row[0]}: {product_code}--{product_name}'
    except Exception as err:
        print(err)
        return False

try:
    from greenstalk import Client
    beanstalk = Client(('192.168.20.106', 11300), watch='data_preprocess_sending_tradmap')

    def consume_job():
        print('start')
        while True:
            print('reserve...')
            job = beanstalk.reserve()
            if (res := process_job(job.body)):
                beanstalk.delete(job)
                print(res)
            else:
                beanstalk.bury(job) 
            sleep(0.3)
    consume_job()

except KeyboardInterrupt:
    print('exit')
    beanstalk.close()
    