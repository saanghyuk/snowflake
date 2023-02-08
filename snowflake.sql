// 데이터베이스 재가동
//alter warehouse development resume;
CREATE OR REPLACE DATABASE consumer_int
COMMENT = "My first consumer data warehouse";


CREATE DATABASE IF NOT EXISTS consumer int
COMMENT = "My first consumer data warehouse";

create or replace database consumer_prod
data_retention_time_in_days = 10
comment = "Production database of 10 days retention";

// 현재 존재하는 데이터베이스 확인
SHOW DATABASES;
SHOW DATABASES LIKE 'consumer%';


// TRANSIENT DB
CREATE OR REPLACE TRANSIENT DATABASE transientdb
DATA_RETENTION_TIME_IN_DAYS = 0;

// Schema
SHOW SCHEMAS IN DATABASE consumer_int;
use consumer_int;
create or replace schema dwh
comment = "my first schema";


// Transient Schema
create or replace transient schema transientschema
data_retention_time_in_days = 0; // default 1

show schemas like 'dwh%' in database consumer_int;

drop schema transientdb1;
comment = "my first schema";

// Create table
USE consumer_int;
create or replace table dwh.customer (
  C_CUSTKEY Number(38,0),
  C_NAME VARCHAR(25),
  C_ADDRESS VARCHAR(40),
  C_NATIONKEY NUMBER(38,0),
  C_PHONE VARCHAR(15),
  C_ACCTBAL VARCHAR(15),
  C_MKTSEGMENT VARCHAR(10),
  C_COMMENT VARCHAR(117)
);


INSERT INTO CONSUMER_INT.DWH.customer
VALUES (1, 'TAI KIM', 'ACRO FOREST', 1, '123-456-7890', 200, 'AMER', 'Genius');

SELECT * FROM CONSUMER_INT.dwh.CUSTOMER;

// CTAS 구문이라고 말한다.
// Copy with data (DEEP COPY)
CREATE OR REPLACE TABLE "CONSUMER_INT"."DWH"."CUSTOMER_DEEP_COPY"
AS
SELECT * FROM "CONSUMER_INT"."DWH"."CUSTOMER";

// Copy without data(SHALLOW COPY)
CREATE OR REPLACE TABLE "CONSUMER_INT"."DWH"."CUSTOMER_SHALLOW_COPY1"
AS
SELECT * FROM "CONSUMER_INT"."DWH"."CUSTOMER"
WHERE 1=0;

CREATE OR REPLACE TABLE "CONSUMER_INT"."DWH"."CUSTOMER_SHALLOW_COPY2"
LIKE "CONSUMER_INT"."DWH"."CUSTOMER";



SHOW parameters;
SHOW parameters IN DATABASE consumer_int;
SHOW parameters IN WAREHOUSE development;
SHOW parameters IN TABLE "CONSUMER_INT"."DWH"."CUSTOMER";
