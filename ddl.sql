-- SQL Server

DROP TABLE IF EXISTS dbo.transactions;

CREATE TABLE  dbo.transactions (
    row_counter: INTEGER,
    transaction_id INTEGER,
    customer_id INTEGER,
    transaction_date DATE,
    transaction_amount FLOAT
);



ALTER TABLE dbo.transactions 
DROP COLUMN row_counter;