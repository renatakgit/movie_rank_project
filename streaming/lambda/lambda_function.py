import boto3
import json
import datetime
import logging


logger = logging.getLogger()
logger.setLevel("INFO")

stream_name = 'reviews'
bucket =  '${ACCOUNT_ID}-landing-zone'
key = 'sample.json'

s3_client = boto3.resource('s3')
kinesis_client = boto3.client('kinesis', region_name='us-east-1')



def lambda_handler(event, context):
    logger.info("Lambda invoked")

    obj = s3_client.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    for record in json_data:
        review_id = record["review_id"]
        reviewer = record["reviewer"]
        movie = record["movie"]
        rating = record["rating"]
        review_summary = record["review_summary"]
        review_date = record["review_date"]
        spoiler_tag = record["spoiler_tag"]
        review_detail = record["review_detail"]
        spoiler_tag = record["spoiler_tag"]
        helpful = record["helpful"]

        put_to_stream(review_id, reviewer, movie, rating, review_summary, review_date, spoiler_tag, review_detail, helpful)
    logger.info("Lambda finished")


def put_to_stream(review_id, reviewer, movie, rating, review_summary, review_date, spoiler_tag, review_detail, helpful):
    payload = {
        "review_id": review_id,
        "reviewer": reviewer,
        "movie": movie,
        "rating": rating,
        "review_summary": review_summary,
        "review_date": review_date,
        "spoiler_tag": spoiler_tag,
        "review_detail": review_detail,
        "helpful": helpful,
        "event_time": datetime.datetime.now().isoformat()
    }

    logger.info(payload)

    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(payload),
        PartitionKey=review_id)

    logger.info(response)