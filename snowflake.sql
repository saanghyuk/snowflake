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


CREATE OR REPLACE TABLE CONSUMER_INT.DWH.CUSTOMER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

CREATE OR REPLACE TABLE CONSUMER_INT.DWH.ORDERS
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;


USE CONSUMER_INT;

SELECT get_ddl('TABLE', 'dwh.customer');
SELECT get_ddl('TABLE', 'dwh.orders');

// PREVIEW
SELECT * FROM ORDERS LIMIT 10;

SELECT TOP 5 o.o_custkey, COUNT(o.o_orderkey)
FROM CUSTOMER c INNER JOIN ORDERS o ON c.c_custkey = o.o_custkey
GROUP BY o.o_custkey ORDER BY 2 DESC;




CREATE OR REPLACE VIEW top_customer_view(cust_name, cust_address, num_orders) AS
(WITH top_orders(customer_key, num_orders)
AS (
SELECT TOP 5 o.o_custkey, COUNT(o.o_orderkey)
FROM CUSTOMER c INNER JOIN ORDERS o ON c.c_custkey = o.o_custkey
GROUP BY o.o_custkey ORDER BY 2 DESC
)
SELECT c.c_name, c.c_address, t.num_orders
FROM CUSTOMER c JOIN TOP_ORDERS t ON c.c_custkey=t.customer_key
);

SELECT * FROM top_customer_view;


SELECT cust_name, max(num_orders) as num_orders
FROM top_customer_view
GROUP BY cust_name
ORDER BY 2 DESC;

DESCRIBE VIEW top_customer_view;



CREATE TABLE IF NOT EXISTS hospital (
    patient_id integer,
    patient_name varchar,
    billing_address varchar,
    diagnosis varchar,
    treatment varchar,
    cost number(10, 2)
);

INSERT INTO hospital
       (patient_id, patient_name, billing_address, diagnosis, treatment, cost)
    VALUES
        (1, 'Mark', '1982 Songdo', 'Industrial Disease', 'A week of peace and quiet', 2000.00),
        (2, 'Noel', 'Haesong-ro', 'Python Bite', 'anti-venom', 70000.00);


SELECT * FROM hospital;


CREATE VIEW IF NOT EXISTS doctor_view AS
    SELECT patient_id, patient_name, diagnosis, treatment
    FROM hospital;

CREATE VIEW IF NOT EXISTS accountant_view AS
    SELECT patient_id, patient_name, billing_address, cost
    FROM hospital;

SELECT * FROM doctor_view;
SELECT * FROM accountant_view;

SELECT DISTINCT diagnosis FROM doctor_view;

SELECT treatment, cost
FROM doctor_view as dv
    INNER JOIN accountant_view AS av
    ON av.patient_id = dv.patient_id;

SELECT *
FROM consumer_int.dwh.top_customer_view;


SELECT *
FROM consumer_int.information_schema.views
WHERE TABLE_NAME = 'TOP_CUSTOMER_VIEW';

SELECT 1 AS n;
SELECT 1 AS n UNION ALL SELECT n+1 FROM NUMBERS WHERE n<5;

SELECT 1 AS n;
WITH NUMBERS AS (SELECT 1 AS n
                 UNION ALL
                 SELECT n+1
                 FROM NUMBERS
                 WHERE n<5
                )
SELECT * FROM NUMBERS;
