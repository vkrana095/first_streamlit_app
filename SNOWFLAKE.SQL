-- select 'Hello';
-- select 'Hello' as "Greeting";
-- Show databasUTIL_DBes;
-- show tables;
-- show schemas;
-- SELECT * 
-- FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA;
-- Typo:     ALTER SCHEMA GARDEN_PLANTS.WEGGIES RENAME TO GARDEN_PLANTS.VEGGIES;

-- Wrong Place:   ALTER SCHEMA DEMO_DB.VEGGIES RENAME TO GARDEN_PLANTS.VEGGIES;

-- SELECT * 
-- FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
-- where schema_name in ('FLOWERS','FRUITS','VEGGIES');

SELECT count(*) as SCHEMAS_FOUND, '3' as SCHEMAS_EXPECTED 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');

-- create table vegetable_details
-- (
-- plant_name varchar(25)
-- , root_depth_code varchar(1)    
-- );
select * from vegetable_details;
-- delete from vegetable_details
-- where plant_name = 'Spinach'
-- and root_depth_code = 'D';


-- INSERT INTO ROOT_DEPTH (
-- 	ROOT_DEPTH_ID ,
-- 	ROOT_DEPTH_CODE ,
-- 	ROOT_DEPTH_NAME ,
-- 	UNIT_OF_MEASURE ,
-- 	RANGE_MIN ,
-- 	RANGE_MAX 
-- )

-- VALUES
-- (2,'M','Medium','cm',45,60)
-- (3,'D','Deep','cm',60,90)
-- ;
Select * from root_depth

-- insert into root_depth (root_depth_id, root_depth_code
--                         , root_depth_name, unit_of_measure
--                         , range_min, range_max)  
-- values
--  (3,'D','Deep','cm',60,90)
-- ,(2,'M','Medium','cm',45,60)
-- ;

-- create or replace table GARDEN_PLANTS.VEGGIES.ROOT_DEPTH (
--    ROOT_DEPTH_ID number(1), 
--    ROOT_DEPTH_CODE text(1), 
--    ROOT_DEPTH_NAME text(7), 
--    UNIT_OF_MEASURE text(2),
--    RANGE_MIN number(2),
--    RANGE_MAX number(2)
--    );
INSERT INTO ROOT_DEPTH (
	ROOT_DEPTH_ID ,
	ROOT_DEPTH_CODE ,
	ROOT_DEPTH_NAME ,
	UNIT_OF_MEASURE ,
	RANGE_MIN ,
	RANGE_MAX 
)

VALUES
(
    1,
    'S',
    'Shallow',
    'cm',
    30,
    45
)
;

-- create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
--     TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
--     FIELD_DELIMITER = '|' --pipes as column separators
--     SKIP_HEADER = 1 --one header row to skip
--     ;

-- create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
--     TYPE = 'CSV'--csv for comma separated files
--     SKIP_HEADER = 1 --one header row  
--     FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
--this means that some values will be wrapped in double-quotes bc they have commas in them
--     ;

-- show file formats;

create file format garden_plants.veggies.L8_CHALLENGE_FF 
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = '\t' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    ;
    
     -- create stage garden_plants.veggies.like_a_window_into_an_s3_bucket 
 -- url = 's3://uni-lab-files';
 -- list @like_a_window_into_an_s3_Bucket;
 -- list @like_a_window_into_an_s3_bucket/this_;
 -- list @like_a_window_into_an_s3_bucket/THIS_;

--  create or replace table vegetable_details_soil_type
-- ( plant_name varchar(25)
--  ,soil_type number(1,0)
-- );

-- copy into vegetable_details_soil_type
-- from @like_a_window_into_an_s3_bucket
-- files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
-- file_format = ( format_name=PIPECOLSEP_ONEHEADROW );

copy into VEGETABLE_DETAILS_PLANT_HEIGHT
from @like_a_window_into_an_s3_bucket
files = ('veg_plant_height.csv')
file_format = ( format_name=COMMASEP_DBLQUOT_ONEHEADROW);

-- --The data in the file, with no FILE FORMAT specified
-- select $1
-- from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv;

-- --Same file but with one of the file formats we created earlier  
-- select $1, $2, $3
-- from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
-- (file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

-- --Same file but with the other file format we created earlier
-- select $1, $2, $3
-- from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
-- (file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );

-- select $1, $2, $3
-- from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
-- (file_format => garden_plants.veggies.L8_CHALLENGE_FF );

-- create or replace table LU_SOIL_TYPE(
-- SOIL_TYPE_ID number,	
-- SOIL_TYPE varchar(15),
-- SOIL_DESCRIPTION varchar(75)
--  );
-- copy into LU_SOIL_TYPE
-- from @like_a_window_into_an_s3_bucket
-- files = ( 'LU_SOIL_TYPE.tsv')
-- file_format = ( format_name=L8_CHALLENGE_FF );

-- create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT(
-- PLANT_NAME varchar(30),
-- UOM varchar(2),	
-- Low_End_of_Range number,
-- High_End_of_Range number
--  );

select *
from @garden_plants.veggies.like_a_window_into_an_s3_bucket/veg_plant_height.csv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

-- use role sysadmin;

-- // Create a new database and set the context to use the new database
-- CREATE DATABASE LIBRARY_CARD_CATALOG COMMENT = 'DWW Lesson 9 ';
-- USE DATABASE LIBRARY_CARD_CATALOG;

-- // Create Author table
-- CREATE OR REPLACE TABLE AUTHOR (
--    AUTHOR_UID NUMBER 
--   ,FIRST_NAME VARCHAR(50)
--   ,MIDDLE_NAME VARCHAR(50)
--   ,LAST_NAME VARCHAR(50)
-- );

-- // Insert the first two authors into the Author table
-- INSERT INTO AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
-- Values
-- (1, 'Fiona', '','Macdonald')
-- ,(2, 'Gian','Paulo','Faleschini');

-- // Look at your table with it's new rows
-- SELECT * 
-- FROM AUTHOR;

-- USE DATABASE LIBRARY_CARD_CATALOG;

-- // Create a new sequence, this one will be a counter for the book table
-- CREATE OR REPLACE SEQUENCE "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_BOOK_UID" 
-- START 1 
-- INCREMENT 1 
-- COMMENT = 'Use this to fill in the BOOK_UID everytime you add a row';

// Create the book table and use the NEXTVAL as the 
// default value each time a row is added to the table
-- CREATE OR REPLACE TABLE BOOK
-- ( BOOK_UID NUMBER DEFAULT SEQ_BOOK_UID.nextval
--  ,TITLE VARCHAR(50)
--  ,YEAR_PUBLISHED NUMBER(4,0)
-- );

// Insert records into the book table
// You don't have to list anything for the
// BOOK_UID field because the default setting
// will take care of it for you
-- INSERT INTO BOOK(TITLE,YEAR_PUBLISHED)
-- VALUES
--  ('Food',2001)
-- ,('Food',2006)
-- ,('Food',2008)
-- ,('Food',2016)
-- ,('Food',2015);

// Create the relationships table
// this is sometimes called a "Many-to-Many table"
-- CREATE TABLE BOOK_TO_AUTHOR
-- (  BOOK_UID NUMBER
--   ,AUTHOR_UID NUMBER
-- );

//Insert rows of the known relationships
-- INSERT INTO BOOK_TO_AUTHOR(BOOK_UID,AUTHOR_UID)
-- VALUES
--  (1,1) // This row links the 2001 book to Fiona Macdonald
-- ,(1,2) // This row links the 2001 book to Gian Paulo Faleschini
-- ,(2,3) // Links 2006 book to Laura K Egendorf
-- ,(3,4) // Links 2008 book to Jan Grover
-- ,(4,5) // Links 2016 book to Jennifer Clapp
-- ,(5,6);// Links 2015 book to Kathleen Petelinsek


//Check your work by joining the 3 tables together
//You should get 1 row for every author
-- select * 
-- from book_to_author ba 
-- join author a 
-- on ba.author_uid = a.author_uid 
-- join book b 
-- on b.book_uid=ba.book_uid; 

// JSON DDL Scripts
-- USE LIBRARY_CARD_CATALOG;

-- // Create an Ingestion Table for JSON Data
-- CREATE TABLE LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON 
-- (
--   RAW_AUTHOR VARIANT
-- );

-- //Create File Format for JSON Data
-- CREATE FILE FORMAT LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT 
-- TYPE = 'JSON' 
-- COMPRESSION = 'AUTO' 
-- ENABLE_OCTAL = FALSE
-- ALLOW_DUPLICATE = FALSE
-- STRIP_OUTER_ARRAY = TRUE
-- STRIP_NULL_VALUES = FALSE
-- IGNORE_UTF8_ERRORS = FALSE; 

-- list @like_a_window_into_an_s3_Bucket/author;

-- copy into LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON
-- from @like_a_window_into_an_s3_Bucket
-- files = ('author_with_header.json')
-- file_format = ( format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT );

-- select raw_author from AUTHOR_INGEST_JSON;

//returns AUTHOR_UID value from top-level object's attribute
-- select raw_author:AUTHOR_UID
-- from author_ingest_json;

//returns the data in a way that makes it look like a normalized table
-- SELECT 
--  raw_author:AUTHOR_UID
-- ,raw_author:FIRST_NAME::STRING as FIRST_NAME
-- ,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
-- ,raw_author:LAST_NAME::STRING as LAST_NAME
-- FROM AUTHOR_INGEST_JSON;
// Create an Ingestion Table for the NESTED JSON Data
-- CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON 
-- (
--   "RAW_NESTED_BOOK" VARIANT
-- );

-- copy into LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON
-- from @like_a_window_into_an_s3_Bucket
-- files = ('json_book_author_nested.json')
-- file_format = ( format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT );

SELECT RAW_NESTED_BOOK
FROM NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:year_published
FROM NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:authors
FROM NESTED_INGEST_JSON;

//try changing the number in the brackets to return authors from a different row
SELECT RAW_NESTED_BOOK:authors[0].first_name
FROM NESTED_INGEST_JSON;

//Use these example flatten commands to explore flattening the nested book and author data
SELECT value:first_name
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

SELECT value:first_name
FROM NESTED_INGEST_JSON
,table(flatten(RAW_NESTED_BOOK:authors));

//Add a CAST command to the fields returned
SELECT value:first_name::VARCHAR, value:last_name::VARCHAR
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

//Assign new column  names to the columns using "AS"
SELECT value:first_name::VARCHAR AS FIRST_NM
, value:last_name::VARCHAR AS LAST_NM
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

-- create sequence SEQ_AUTHOR_UID
--     start = 1
--     increment = 1
--     comment = 'Use this to fill in author UID';
-- use role sysadmin;

//See how the nextval function works
-- SELECT SEQ_AUTHOR_UID.nextval, SEQ_AUTHOR_UID.nextval;
-- SHOW sequences;

//Drop and recreate the counter (sequence) so that it starts at 3 
// then we'll add the other author records to our author table
-- CREATE OR REPLACE SEQUENCE "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_AUTHOR_UID" 
-- START 3 
-- INCREMENT 1 
-- COMMENT = 'Use this to fill in the AUTHOR_UID every time you add a row';

-- INSERT INTO AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
-- Values
-- (SEQ_AUTHOR_UID.nextval, 'Laura', 'K','Egendorf')
-- ,(SEQ_AUTHOR_UID.nextval, 'Jan', '','Grover')
-- ,(SEQ_AUTHOR_UID.nextval, 'Jennifer', '','Clapp')
-- ,(SEQ_AUTHOR_UID.nextval, 'Kathleen', '','Petelinsek');

//Create a new database to hold the Twitter file
CREATE DATABASE SOCIAL_MEDIA_FLOODGATES 
COMMENT = 'There\'s so much data from social media - flood warning';

USE DATABASE SOCIAL_MEDIA_FLOODGATES;

//Create a table in the new database
CREATE TABLE SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST 
("RAW_STATUS" VARIANT) 
COMMENT = 'Bring in tweets, one row per tweet or status entity';

//Create a JSON file format in the new database
CREATE FILE FORMAT SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT 
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = FALSE 
ALLOW_DUPLICATE = FALSE 
STRIP_OUTER_ARRAY = TRUE 
STRIP_NULL_VALUES = FALSE 
IGNORE_UTF8_ERRORS = FALSE;


-- copy into SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
-- from @like_a_window_into_an_s3_Bucket
-- files = ('nutrition_tweets.json')
-- file_format = ( format_name=SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT );

//select statements as seen in the video
SELECT RAW_STATUS
FROM TWEET_INGEST;

SELECT RAW_STATUS:entities
FROM TWEET_INGEST;

SELECT RAW_STATUS:entities:hashtags
FROM TWEET_INGEST;

//Explore looking at specific hashtags by adding bracketed numbers
//This query returns just the first hashtag in each tweet
SELECT RAW_STATUS:entities:hashtags[0].text
FROM TWEET_INGEST;

//This version adds a WHERE clause to get rid of any tweet that 
//doesn't include any hashtags
SELECT RAW_STATUS:entities:hashtags[0].text
FROM TWEET_INGEST
WHERE RAW_STATUS:entities:hashtags[0].text is not null;

//Perform a simple CAST on the created_at key
//Add an ORDER BY clause to sort by the tweet's creation date
SELECT RAW_STATUS:created_at::DATE
FROM TWEET_INGEST
ORDER BY RAW_STATUS:created_at::DATE;

//Flatten statements that return the whole hashtag entity
SELECT value
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

SELECT value
FROM TWEET_INGEST
,TABLE(FLATTEN(RAW_STATUS:entities:hashtags));

//Flatten statement that restricts the value to just the TEXT of the hashtag
SELECT value:text
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);


//Flatten and return just the hashtag text, CAST the text as VARCHAR
SELECT value:text::VARCHAR
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

//Flatten and return just the hashtag text, CAST the text as VARCHAR
// Use the AS command to name the column
SELECT value:text::VARCHAR AS THE_HASHTAG
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

//Add the Tweet ID and User ID to the returned table
SELECT RAW_STATUS:user:id AS USER_ID
,RAW_STATUS:id AS TWEET_ID
,value:text::VARCHAR AS HASHTAG_TEXT
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

create or replace view SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED as
(SELECT RAW_STATUS:user:id AS USER_ID
,RAW_STATUS:id AS TWEET_ID
,value:text::VARCHAR AS HASHTAG_TEXT
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags)
);

-- select current_region();
-- select current_account();
-- set mystery_bag = 'It is Empty?';
-- select $mystery_bag;
-- SET var1 = 1;
-- SET var2 = 3;
-- SET var3 = 4;

-- SELECT $var1+$var2+$var3;

-- Create function SUM_MYSTERY_BAG_VARS(var1 Number,var2 Number,var3 Number)
-- returns number as 'select var1+var2+var3';


-- select SUM_MYSTERY_BAG_VARS(2,4,6);

-- SET that = -10.5;
-- SET this = 2;
-- SET the_other = 1000;

-- create function NEUTRALIZE_WHINING(var1 text)
-- Returns text as 'select initcap(var1)';


-- select NEUTRALIZE_WHINING('who ARe yoU?');

Show stages in account;

Create stage my_internal_vivek_stage; 

list @MY_INTERNAL_SNOWFLAKE_STAGE; 

select $1 from @my_internal_vivek_stage/my_file.txt.gz;

Alter stage @my_internal_vivek_stage;
-- use role pc_rivery_role;
-- use warehouse pc_rivery_wh;

-- create or replace TABLE PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST (
-- 	FRUIT_NAME VARCHAR(25)
-- );


-- insert into PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST
-- values ('banana')
-- , ('cherry')
-- , ('strawberry')
-- , ('pineapple')
-- , ('apple')
-- , ('mango')
-- , ('coconut')
-- , ('plum')
-- , ('avocado')
-- , ('starfruit');

-- insert into PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST values ('test');

-- select * from PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST;


-- delete from PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST
-- where fruit_name = 'test'
-- or fruit_name = 'from streamlit';

drop database snowflake_sample_data;

alter database GET_RETRIEVED_SAMPLE_DATA
rename to snowflake_sample_data;

GRANT IMPORTED PRIVILEGES
ON DATABASE SNOWFLAKE_SAMPLE_DATA
TO ROLE SYSADMIN;

--Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT c_mktsegment, COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY c_mktsegment
ORDER BY COUNT(*);


-- Nations Table
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table
SELECT R_REGIONKEY, R_NAME
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Join the Tables and Sort
SELECT R_NAME as Region, N_NAME as Nation
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
ORDER BY R_NAME, N_NAME ASC;

--Group and Count Rows Per Region
SELECT R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
GROUP BY R_NAME;


USE ROLE SYSADMIN;
CREATE DATABASE INTL_DB;
USE SCHEMA INTL_DB.PUBLIC;


CREATE WAREHOUSE INTL_WH 
WITH WAREHOUSE_SIZE = 'XSMALL' 
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 600 
AUTO_RESUME = TRUE;

USE WAREHOUSE INTL_WH;

create database ZENAS_ATHLEISURE_DB;

create schema ZENAS_ATHLEISURE_DB.PRODUCTS;

list @uni_klaus_clothing;

CREATE STAGE "UNI_KLAUS_ZMD" 
	URL = 's3://uni-klaus/zenas_metadata' 
	DIRECTORY = ( ENABLE = true );

list @UNI_KLAUS_ZMD;

CREATE STAGE "UNI_KLAUS_SNEAKERS" 
	URL = 's3://uni-klaus/sneakers' 
	DIRECTORY = ( ENABLE = true );

list @UNI_KLAUS_SNEAKERS;

select $1 from @UNI_KLAUS_ZMD/sweatsuit_sizes.txt;

create file format zmd_file_format_1
RECORD_DELIMITER = '^';

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

create file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^';

drop file format zmd_file_format_3;

create view SWEATBAND_COORDINATION as
select $1 as PRODUCT_CODE,$2 as HAS_MATCHING_SWEATSUIT
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';';

create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = True;

create view sweatsuit_sizes as
select REPLACE($1,chr(13)||chr(10)) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';

Create view SWEATBAND_PRODUCT_LINE as
select REPLACE($1,chr(13)||chr(10)) as PRODUCT_CODE, $2 as HEADBAND_DESCRIPTION, $3 as WRISTBAND_DESCRIPTION
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2 );

-- create view sweatsuit_sizes as 
select * from sweatsuit_sizes;

select * from SWEATBAND_PRODUCT_LINE;

select * from SWEATBAND_COORDINATION;

select $1
from @uni_klaus_clothing/90s_tracksuit.png; 

select metadata$filename, count(metadata$file_row_number) as Number_of_rows
from @uni_klaus_clothing
Group by 1;


--Directory Tables
select * from directory(@uni_klaus_clothing);

-- Oh Yeah! We have to turn them on, first
alter stage uni_klaus_clothing 
set directory = (enable = true);

--Now?
select * from directory(@uni_klaus_clothing);

--Oh Yeah! Then we have to refresh the directory table!
alter stage uni_klaus_clothing refresh;

--Now?
select * from directory(@uni_klaus_clothing);


--testing UPPER and REPLACE functions on directory table
select UPPER(RELATIVE_PATH) as uppercase_filename
, REPLACE(uppercase_filename,'/') as no_slash_filename
, REPLACE(no_slash_filename,'_',' ') as no_underscores_filename
, REPLACE(no_underscores_filename,'.PNG') as just_words_filename
from directory(@uni_klaus_clothing);

select REPLACE(REPLACE(REPLACE(UPPER(RELATIVE_PATH),'/'),'_',' '),'.PNG')
from directory(@uni_klaus_clothing);

--create an internal table for some sweat suit info
create or replace TABLE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS (
	COLOR_OR_STYLE VARCHAR(25),
	DIRECT_URL VARCHAR(200),
	PRICE NUMBER(5,2)
);

--fill the new table with some data
insert into  ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS 
          (COLOR_OR_STYLE, DIRECT_URL, PRICE)
values
('90s', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/90s_tracksuit.png',500)
,('Burgundy', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/forest_green_sweatsuit.png',65)
,('Navy Blue', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/navy_blue_sweatsuit.png',65)
,('Orange', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/orange_sweatsuit.png',65)
,('Pink', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/pink_sweatsuit.png',65)
,('Purple', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/purple_sweatsuit.png',65)
,('Red', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/red_sweatsuit.png',65)
,('Royal Blue',	'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/royal_blue_sweatsuit.png',65)
,('Yellow', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/yellow_sweatsuit.png',65);


select color_or_style,
direct_url,
price,
size as image_size,
last_modified as image_last_modified
from sweatsuits s
join directory(@uni_klaus_clothing) d
-- on split_part(s.DIRECT_URL,'/',-1)=split_part(d.relative_path,'/',-1);
-- on split_part(s.DIRECT_URL, '/', 5) = replace(d.RELATIVE_PATH, '/');
on d.relative_path = SUBSTR(s.direct_url,54,50);


select REPLACE(REPLACE(REPLACE(REPLACE(UPPER(RELATIVE_PATH),'/'),'_TRACKSUIT.PNG',' '),'_SWEATSUIT.PNG',''),'_',' ')
from directory(@uni_klaus_clothing);


create or replace view CATALOG as
-- 3 way join - internal table, directory table, and view based on external data
select color_or_style
, direct_url
, price
, size as image_size
, last_modified as image_last_modified
, sizes_available
from sweatsuits 
join directory(@uni_klaus_clothing) 
on relative_path = SUBSTR(direZENAS_ATHLEISURE_DB.INFORMATION_SCHEMA.VIEWSct_url,54,50)
cross join sweatsuit_sizes;

DROP VIEW CATALOGUE;


-- Add a table to map the sweat suits to the sweat band sets
create or replace table ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE varchar(25)
,UPSELL_PRODUCT_CODE varchar(10)
);

--populate the upsell table
insert into ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE
,UPSELL_PRODUCT_CODE 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,direct_url
,size_list
,coalesce('BONUS: ' ||  headband_description || ' & ' || wristband_description, 'Consider White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, direct_url, image_last_modified,image_size
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, direct_url, image_last_modified, image_size
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code
where price < 200 -- high priced items like vintage sweatsuits aren't a good fit for this website
and image_size < 1000000 -- large images need to be processed to a smaller size
;

-- CHAPTER -5

Create database MELS_SMOOTHIE_CHALLENGE_DB;
Create schema MELS_SMOOTHIE_CHALLENGE_DB.TRAILS;
DROP SCHEMA MELS_SMOOTHIE_CHALLENGE_DB.PUBLIC;

list @s3:\\uni-lab-files-more;

list @trails_parquet;
list @trails_geojson;

select $1
from @trails_geojson
(file_format => ff_json);

select $1
from @trails_parquet
(file_format => ff_parquet);

select $1:sequence_1 as sequence_1,
$1:trail_name::varchar as trail_name,
$1:latitude as latitude,
$1:longitude as longitude,
$1:sequence_2 as sequence_2,
$1:elevation as elevation
from @trails_parquet
(file_format => ff_parquet)
Order by 1;

--Nicely formatted trail data
create view CHERRY_CREEK_TRAIL as 
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

--Using concatenate to prepare the data for plotting on a map
select top 100 
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
-- where point_id <= 10
group by trail_name;

create view DENVER_AREA_TRAILS as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

--CHAPTER 7

--Remember this code? 
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,st_length(TO_GEOGRAPHY(my_linestring)) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail
group by trail_name;

select feature_name,st_length(TO_GEOGRAPHY(geometry)) as trail_length from denver_area_trails;

select * from denver_area_trails;

select get_ddl('view', 'DENVER_AREA_TRAILS');

create or replace view DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,st_length(TO_GEOGRAPHY(geometry)) as trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',')||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name;

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;


--Add more GeoSpatial Calculations to get more GeoSpecial Information! 
Create view TRAILS_AND_BOUNDARIES as
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;


-------------------------Chanpter 8

-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lat='-104.97300245114094';
set mc_lng='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lat='-105.00840763333615'; 
set loc_lng='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lat,$mc_lng) as melanies_cafe_point;
select st_makepoint($loc_lat,$loc_lng) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lat,$mc_lng)
        ,st_makepoint($loc_lat,$loc_lng)
        ) as mc_to_cp;

CREATE OR REPLACE FUNCTION distance_to_mc(loc_lat number(38,32), loc_lng number(38,32))
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,st_makepoint('-105.00532059763648','39.74548137398218')
        )
  $$
  ;

  --Tivoli Center into the variables 
set tc_lat='-105.00532059763648'; 
set tc_lng='39.74548137398218';

select distance_to_mc($tc_lat,$tc_lng);

CREATE VIEW COMPETITION as 
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

-- show views

SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;

CREATE OR REPLACE FUNCTION distance_to_mc(lat_and_lng GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lat_and_lng
        )
  $$
  ;

  SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;

-- Tattered Cover Bookstore McGregor Square
set tcb_lat='-104.9956203'; 
set tcb_lng='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lat,$tcb_lng);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lat,$tcb_lng));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';


-- create view DENVER_BIKE_SHOPS as
select NAME,distance_to_mc(coordinates) as distance_to_melanies 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='bicycle';

alter view cherry_creek_trail
rename to v_cherry_creek_trail;

create or replace external table T_CHERRY_CREEK_TRAIL(
    POINT_ID number as ($1:sequence_1::number),
    TRAIL_NAME varchar(50) as ($1:trail_name::varchar),
    LNG NUMBER(11,8) as ($1:longitude::number(11,8)),
    LAT NUMBER(11,8) as ($1:latitude::number(11,8)),
    COORD_PAIR varchar(50) as (lng::varchar||''||lat::varchar) 
	-- my_filename varchar(50) as (metadata$filename::varchar(50))
) 
location= @trails_parquet
auto_refresh = true
-- file_format = (type = parquet);
file_format = ff_parquet;

create or replace external table mels_smoothie_challenge_db.trails.T_CHERRY_CREEK_TRAIL(
	POINT_ID number as ($1:sequence_1::number),
	TRAIL_NAME varchar(50) as  ($1:trail_name::varchar),
	LNG number(11,8) as ($1:latitude::number(11,8)),
	LAT number(11,8) as ($1:longitude::number(11,8)),
	COORD_PAIR varchar(50) as (lng::varchar||' '||lat::varchar)
) 
location= @mels_smoothie_challenge_db.trails.trails_parquet
auto_refresh = true
file_format = mels_smoothie_challenge_db.trails.ff_parquet;

select get_ddl('view','mels_smoothie_challenge_db.trails.v_cherry_creek_trail');
select get_ddl('view','mels_smoothie_challenge_db.trails.t_cherry_creek_trail');


create or replace view V_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR
) as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

select * from T_CHERRY_CREEK_TRAIL;

