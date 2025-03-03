CREATE EXTERNAL TABLE `reviews` (
  `helpful` array<bigint>,
  `movie` string,
  `rating` bigint,                 
  `review_detail` string,
  `review_id` string,
  `review_summary` string,
  `reviewer` string,
  `spoiler_tag` boolean,
  `review_date` date
)
PARTITIONED BY (review_month string)  
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://975049959409-formatted-data/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0',
  'CrawlerSchemaSerializerVersion'='1.0',
  'UPDATED_BY_CRAWLER'='review_crawler',
  'averageRecordSize'='1459',
  'classification'='parquet',
  'compressionType'='Snappy',
  'objectCount'='1',
  'recordCount'='100000',
  'sizeKey'='94807077',
  'typeOfData'='file');

 MSCK REPAIR TABLE reviews;