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
PRIMARY KEY(W_ID)) CSV DATA ('nodelocal://self/project-files/data-files/warehouse.csv');
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
PRIMARY KEY(D_W_ID, D_ID)) CSV DATA ('nodelocal://self/project-files/data-files/district.csv');
--ALTER TABLE DISTRICT ADD CONSTRAINT DISTRICT_W_ID FOREIGN KEY (D_W_ID) REFERENCES WAREHOUSE(W_ID);
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
PRIMARY KEY(C_W_ID, C_D_ID, C_ID)) CSV DATA ('nodelocal://self/project-files/data-files/customer.csv');
--ALTER TABLE CUSTOMER ADD CONSTRAINT CUSTOMER_W_ID_D_ID FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES DISTRICT(D_W_ID, D_ID);
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
PRIMARY KEY(O_W_ID, O_D_ID, O_ID)) CSV DATA ('nodelocal://self/project-files/data-files/order.csv') WITH nullif = 'null';
--CREATE INDEX ON ORDERS(O_W_ID, O_D_ID, O_C_ID);
--ALTER TABLE ORDERS ADD CONSTRAINT ORDERS_W_ID_D_ID_C_ID FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID);
DROP TABLE IF EXISTS ITEM;
IMPORT TABLE ITEM (
I_ID INT,
I_NAME VARCHAR(24),
I_PRICE DECIMAL(5,2),
I_IM_ID INT,
I_DATA VARCHAR(50),
PRIMARY KEY(I_ID)) CSV DATA ('nodelocal://self/project-files/data-files/item.csv');
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
PRIMARY KEY(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)) CSV DATA ('nodelocal://self/project-files/data-files/order-line.csv') WITH nullif = 'null';
--ALTER TABLE ORDERLINE ADD CONSTRAINT ORDERLINE_W_ID_D_ID_O_ID FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES ORDERS(O_W_ID, O_D_ID, O_ID);
--CREATE INDEX ON ORDERLINE(OL_I_ID);
--ALTER TABLE ORDERLINE ADD CONSTRAINT ORDERLINE_I_ID FOREIGN KEY (OL_I_ID) REFERENCES ITEM(I_ID);
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
PRIMARY KEY(S_W_ID, S_I_ID)) CSV DATA ('nodelocal://self/project-files/data-files/stock.csv');
--ALTER TABLE STOCK ADD CONSTRAINT STOCK_W_ID FOREIGN KEY (S_W_ID) REFERENCES WAREHOUSE(W_ID);
--CREATE INDEX ON STOCK(S_I_ID);
--ALTER TABLE STOCK ADD CONSTRAINT STOCK_I_ID FOREIGN KEY (S_I_ID) REFERENCES ITEM(I_ID);










