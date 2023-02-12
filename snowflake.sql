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




CREATE OR REPLACE VIEW top_customer_view (cust_name, cust_address, num_orders) AS
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


CREATE OR REPLACE RECURSIVE VIEW r_view (n)
AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n+1
  FROM r_view
  WHERE n < 5
);

SELECT * FROM r_view;


CREATE OR REPLACE materialized VIEW open_orders (customer_key, num_orders)
AS (
    SELECT o_custkey, o_orderkey
    FROM orders
    WHERE o_orderstatus = 'O'
);

SELECT * FROM open_orders;


//TEMPORARY, TRANSIENT TABLES
CREATE OR REPLACE TEMPORARY TABLE my_temporary_table(col1 int, col2 string)
DATA_RETENTION_TIME_IN_DAYS = 1; // value can be 0 or 1

SELECT * FROM my_temporary_table;

SHOW databases;

CREATE OR REPLACE TRANSIENT TABLE my_transient_table(col1 int, col2 string)
DATA_RETENTION_TIME_IN_DAYS = 1; // value can be 0 or 1

SELECT * FROM my_transient_table;

// Constraints are not enforced excetp NOT NULL
CREATE OR REPLACE TABLE MY_CONSTRAINT_TABLE
(col1 int PRIMARY KEY, col2 int NOT NULL);

INSERT INTO MY_CONSTRAINT_TABLE
VALUES
(1, 100), (1, 200);

SELECT * FROM MY_CONSTRAINT_TABLE;

INSERT INTO MY_CONSTRAINT_TABLE
VALUES
(1, 100), (1, null);

SELECT * FROM MY_CONSTRAINT_TABLE;


// Context Function
SELECT CURRENT_REGION(),
CURRENT_USER(),
CURRENT_ROLE(),
CURRENT_DATABASE(),
CURRENT_SCHEMA(),
CURRENT_WAREHOUSE(),
CURRENT_TIMESTAMP(),
CURRENT_DATE();


//Data Types
CREATE OR REPLACE TABLE "DWH"."MyDataTypeTable"
(col_num_1 INT, col_num_2 INTEGER, col_num_3 BIGINT, col_num_4 NUMBER, col_num_5 NUMERIC(10, 2), col_num_6 DECIMAL(20, 3),
col_str_1 CHAR, col_str_2 CHAR(4), col_str_3 VARCHAR, col_str_4 STRING, col_str_5 VARCHAR(5), col_str_6 TEXT,
col_bool BOOLEAN,
col_dt_1 DATE, col_dt_2 DATETIME, col_dt_3 TIME, col_dt_4 TIMESTAMP
);

DROP TABLE dwh.student;
SELECT * FROM dwh.student;


CREATE OR REPLACE TABLE dwh.student
   (
     src variant
   )
AS
SELECT parse_json(int1) AS src
FROM values
(54, 3, 1);

SELECT * FROM dwh.student;
DESCRIBE TABLE dwh.student;

CREATE OR REPLACE TABLE dwh.student
   (
     src variant
   )
AS
SELECT parse_json(column1) AS src
FROM values
('
 {
    "school": "CottonWood Elementary",
    "teacher" : {"name":"Tai Kim", "level": "max"},
    "rating": 9,
    "student": [
        {
        "id": "01",
        "name": "Alpha",
        "lastname": "Kim"
         },
        {
        "id": "02",
        "name": "Beta",
        "lastname": "Kim"
        }
     ]
 }');

SELECT * FROM dwh.student;
SELECT src:school, src:teacher.name FROM dwh.student;
SELECT src['school'], src['teacher']['name'] FROM student;
SELECT src:student[0].id, src:student[0].name, src:student[0].lastname FROM dwh.student;


CREATE OR REPLACE TABLE rating(rating) AS
SELECT src:rating::float FROM dwh.student;

SELECT* FROM rating;
DESCRIBE TABLE rating;


SELECT get_ddl('table', 'rating');

SELECT to_variant("string");

DESCRIBE TABLE rating;

SELECT * FROM dwh.student;

INSERT INTO dwh.student SELECT to_variant(5);
SELECT * FROM dwh.student;
DESCRIBE TABLE dwh.student;

// LOAD JSON DATA
CREATE OR REPLACE TABLE dwh.MyVariantTable
(
   col_1 INT AUTOINCREMENT,
   col_2 VARIANT
);

INSERT INTO dwh.MyVariantTable(col_2)
    SELECT parse_json(column1) AS v
    FROM VALUES ('null'), (null), ('true'), ('-17'), ('123.12'), ('1.912e2'), ('"String double quoted"  '), ('[-99, 10, 100, 87, false,]'), ('{"x": "abc", "y": false, "z": 10}') AS vals;

SELECT * FROM dwh.MyVariantTable;

SELECT parse_json(column1)
FROM VALUES ('null'), (null), ('true'), ('-17'), ('123.12'), ('1.912e2'), ('"String double quoted"  '), ('[-99, 10, 100, 87, false,]'), ('{"x": "abc", "y": false, "z": 10}') AS vals;


SELECT col_1, col_2, typeof(col_2)
FROM dwh.MyVariantTable
ORDER BY 1;

create or replace table vartab (n number(2), v variant);
insert into vartab
    select column1 , parse_json(column2)
    from values (1, 'null'),
                (2, null),
                (3, 'true'),
                (4, '-17'),
                (5, '123.12'),
                (6, '1.912e2'),
                (7, '"Om ara pa ca na dhih"  '),
                (8, '[-1, 12, 289, 2188, false,]'),
                (9, '{ "x" : "abc", "y" : false, "z": 10} ')
       as vals;

SELECT * FROM vartab;



CREATE OR REPLACE TABLE autoincrement_table(
    col1 INT AUTOINCREMENT,
    col2 STRING
);

INSERT INTO autoincrement_table(col1, col2) VALUES (10, 'value');
SELECT * FROM autoincrement_table;

// IDENTITY
CREATE OR REPLACE TABLE identityTable(
    col1 INT IDENTITY,
    col2 STRING
);

INSERT INTO identityTable(col2) VALUES ('value');
SELECT * FROM identityTable;


// SEQUENCE
CREATE OR REPLACE SEQUENCE seq1
START WITH = 1
INCREMENT BY = 2;

CREATE OR REPLACE TABLE seq1Tab
(
  customer_id INTEGER,
  customer_name STRING
);

INSERT INTO seq1Tab
VALUES (seq1.nextval, 'val1');

SELECT * FROM seq1Tab;


CREATE OR REPLACE TABLE seq2Tab
(
  customer_id INTEGER DEFAULT seq1.NEXTVAL,
  customer_name STRING
);


INSERT INTO seq2Tab
VALUES (seq1.nextval, 'val1');

INSERT INTO seq2Tab(customer_name)
VALUES ('values');


SELECT * FROM seq2Tab;


CREATE OR REPLACE TABLE source_table (v varchar) AS SELECT 'test' FROM table(generator(rowcount => 10000));
SELECT * FROM source_table;

CREATE OR REPLACE TABLE target_table(id number identity, v varchar);
SELECT * FROM source_table;
SELECT * FROM target_table;

INSERT INTO target_table (v) SELECT v FROM source_table;
INSERT INTO target_table (v) VALUES ('new one');
SELECT * FROM target_table ORDER BY id DESC LIMIT 10;

INSERT INTO target_table (v) SELECT v FROM source_table;
INSERT INTO target_table (v) VALUES ('new one');

SELECT * FROM target_table ORDER BY id DESC LIMIT 10;
