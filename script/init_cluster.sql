ALTER RANGE meta CONFIGURE ZONE USING num_replicas = 3;
DROP DATABASE IF EXISTS WHOLESALE;
CREATE DATABASE WHOLESALE;
SET DATABASE = WHOLESALE;
DROP TABLE IF EXISTS WAREHOUSE;
IMPORT TABLE WAREHOUSE (
W_ID INT,
W_NAME VARCHAR(10),
W_STREET_1 VARCHAR(20),
W_STREET_2 VARCHAR(20),
W_CITY VARCHAR(20),
W_STATE CHAR(2),
W_ZIP CHAR(9),
W_TAX DECIMAL(4,4),
W_YTD DECIMAL(12,2),
PRIMARY KEY(W_ID),
FAMILY W_W (W_ID, W_YTD),
FAMILY W_R (W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX)
) CSV DATA ('nodelocal://1/project-files/data-files/warehouse.csv');
DROP TABLE IF EXISTS DISTRICT;
IMPORT TABLE DISTRICT (
D_W_ID INT,
D_ID INT,
D_NAME VARCHAR(10),
D_STREET_1 VARCHAR(20),
D_STREET_2 VARCHAR(20),
D_CITY VARCHAR(20),
D_STATE CHAR(2),
D_ZIP CHAR(9),
D_TAX DECIMAL(4,4),
D_YTD DECIMAL(12,2),
D_NEXT_O_ID INT,
PRIMARY KEY(D_W_ID, D_ID),
FAMILY D_W (D_W_ID, D_ID, D_YTD, D_NEXT_O_ID),
FAMILY D_R (D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX)
) CSV DATA ('nodelocal://1/project-files/data-files/district.csv');
DROP TABLE IF EXISTS CUSTOMER;
IMPORT TABLE CUSTOMER (
C_W_ID INT,
C_D_ID INT,
C_ID INT,
C_FIRST VARCHAR(16),
C_MIDDLE CHAR(2),
C_LAST VARCHAR(16),
C_STREET_1 VARCHAR(20),
C_STREET_2 VARCHAR(20),
C_CITY VARCHAR(20),
C_STATE CHAR(2),
C_ZIP CHAR(9),
C_PHONE CHAR(16),
C_SINCE TIMESTAMP,
C_CREDIT CHAR(2),
C_CREDIT_LIM DECIMAL(12,2),
C_DISCOUNT DECIMAL(4,4),
C_BALANCE DECIMAL(12,2),
C_YTD_PAYMENT FLOAT,
C_PAYMENT_CNT INT,
C_DELIVERY_CNT INT,
C_DATA VARCHAR(500),
PRIMARY KEY(C_W_ID, C_D_ID, C_ID),
FAMILY C_W (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT),
FAMILY C_R (C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA)
) CSV DATA ('nodelocal://1/project-files/data-files/customer.csv');
DROP TABLE IF EXISTS ORDERS;
IMPORT TABLE ORDERS (
O_W_ID INT,
O_D_ID INT,
O_ID INT,
O_C_ID INT,
O_CARRIER_ID INT,
O_OL_CNT DECIMAL(2,0),
O_ALL_LOCAL DECIMAL(1,0),
O_ENTRY_D TIMESTAMP,
PRIMARY KEY(O_W_ID, O_D_ID, O_ID)
)CSV DATA ('nodelocal://1/project-files/data-files/order.csv') WITH nullif = 'null';
CREATE INDEX ON ORDERS(O_W_ID, O_D_ID, O_C_ID);
DROP TABLE IF EXISTS ITEM;
IMPORT TABLE ITEM (
I_ID INT,
I_NAME VARCHAR(24),
I_PRICE DECIMAL(5,2),
I_IM_ID INT,
I_DATA VARCHAR(50),
PRIMARY KEY(I_ID),
FAMILY I_RO (I_ID, I_PRICE),
FAMILY I_R (I_NAME, I_IM_ID, I_DATA)
) CSV DATA ('nodelocal://1/project-files/data-files/item.csv');
DROP TABLE IF EXISTS ORDERLINE;
IMPORT TABLE ORDERLINE (
OL_W_ID INT,
OL_D_ID INT,
OL_O_ID INT,
OL_NUMBER INT,
OL_I_ID INT,
OL_DELIVERY_D TIMESTAMP,
OL_AMOUNT DECIMAL(6,2),
OL_SUPPLY_W_ID INT,
OL_QUANTITY DECIMAL(2,0),
OL_DIST_INFO CHAR(24),
OL_C_ID INT,
PRIMARY KEY(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
) CSV DATA ('nodelocal://1/project-files/data-files/order-line-new.csv') WITH nullif = 'null';
CREATE INDEX ON ORDERLINE(OL_W_ID, OL_I_ID);
DROP TABLE IF EXISTS STOCK;
IMPORT TABLE STOCK (
S_W_ID INT,
S_I_ID INT,
S_QUANTITY DECIMAL(4,0),
S_YTD DECIMAL(8,2),
S_ORDER_CNT INT,
S_REMOTE_CNT INT,
S_DIST_01 CHAR(24),
S_DIST_02 CHAR(24),
S_DIST_03 CHAR(24),
S_DIST_04 CHAR(24),
S_DIST_05 CHAR(24),
S_DIST_06 CHAR(24),
S_DIST_07 CHAR(24),
S_DIST_08 CHAR(24),
S_DIST_09 CHAR(24),
S_DIST_10 CHAR(24),
S_DATA VARCHAR(50),
PRIMARY KEY(S_W_ID, S_I_ID),
FAMILY S_W (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT),
FAMILY S_R (S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA)
) CSV DATA ('nodelocal://1/project-files/data-files/stock.csv');









