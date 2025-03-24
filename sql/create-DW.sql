CREATE DATABASE IF NOT EXISTS metro_dw;
USE metro_dw;

-- TABLE CREATION
CREATE TABLE output_data (
    order_id INT,
    order_date DATETIME,
    product_id INT,
    quantity_ordered INT,
    customer_id INT,
    customer_name VARCHAR(255),
    gender VARCHAR(10),
    product_name VARCHAR(255),
    product_price DECIMAL(10, 2),
    store_id INT,
    store_name VARCHAR(255),
    supplier_id INT,
    supplier_name VARCHAR(255),
    total_sale DECIMAL(10, 2)
);

select * from output_data;

CREATE TABLE TIME_DIM 
(

    Date_ID INT PRIMARY KEY AUTO_INCREMENT,
    Date DATE NOT NULL,
    Day VARCHAR(10) NOT NULL,
    Month VARCHAR(15) NOT NULL,
    Year INT NOT NULL,
    Is_Weekend BOOLEAN
);

-- PRODUCT dimension
CREATE TABLE PRODUCT 
(
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(255),
    Product_Price DECIMAL(10,2)
);

-- CUSTOMER dimension
CREATE TABLE CUSTOMER 
(
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(255),
    Gender VARCHAR(10)
);

-- STORE dimension
CREATE TABLE STORE
(
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(255)
);

-- SUPPLIER dimension
CREATE TABLE SUPPLIER 
(
    Supplier_ID INT PRIMARY KEY,
    Supplier_Name VARCHAR(255)
);

-- FACT TABLE
CREATE TABLE SALES 
(
    Transaction_ID INT PRIMARY KEY,
    Date_ID INT,
    Product_ID INT,
    Customer_ID INT,
    Store_ID INT,
    Supplier_ID INT,
    Quantity INT,
    Total_Sale DECIMAL(10,2),
    FOREIGN KEY (Date_ID) REFERENCES TIME_DIM(Date_ID),
    FOREIGN KEY (Product_ID) REFERENCES PRODUCT(Product_ID),
    FOREIGN KEY (Customer_ID) REFERENCES CUSTOMER(Customer_ID),
    FOREIGN KEY (Store_ID) REFERENCES STORE(Store_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES SUPPLIER(Supplier_ID)
);

-- INSERTION
INSERT INTO TIME_DIM (Date, Day, Month, Year, Is_Weekend)
SELECT DISTINCT 
    DATE(order_date) AS Date,
    DAYNAME(order_date) AS Day,
    MONTHNAME(order_date) AS Month,
    YEAR(order_date) AS Year,
    CASE 
        WHEN DAYOFWEEK(order_date) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END AS Is_Weekend
FROM output_data;

-- INSERT IN PRODUCT
INSERT INTO PRODUCT (Product_ID, Product_Name, Product_Price)
SELECT DISTINCT 
    product_id AS Product_ID,
    product_name AS Product_Name,
    product_price AS Product_Price
FROM output_data;

-- INSERT IN CUSTOMER
INSERT INTO CUSTOMER (Customer_ID, Customer_Name, Gender)
SELECT DISTINCT 
    customer_id AS Customer_ID,
    customer_name AS Customer_Name,
    gender AS Gender
FROM output_data;

-- INSERT IN STORE
INSERT INTO STORE (Store_ID, Store_Name)
SELECT DISTINCT 
    store_id AS Store_ID,
    store_name AS Store_Name
FROM output_data;

-- INSERT IN SUPPLIER
INSERT IGNORE INTO SUPPLIER (Supplier_ID, Supplier_Name)
SELECT DISTINCT 
    supplier_id AS Supplier_ID,
    supplier_name AS Supplier_Name
FROM output_data;

select count(*) from supplier;

-- INSERT IN SALES
INSERT INTO SALES (Transaction_ID, Date_ID, Product_ID, Customer_ID, Store_ID, Supplier_ID, Quantity, Total_Sale)
SELECT 
    t.order_id AS Transaction_ID,
    td.Date_ID AS Date_ID,
    t.product_id AS Product_ID,
    t.customer_id AS Customer_ID,
    t.store_id AS Store_ID,
    t.supplier_id AS Supplier_ID,
    t.quantity_ordered AS Quantity,
    t.total_sale AS Total_Sale
FROM output_data t
JOIN TIME_DIM td ON td.Date = DATE(t.order_date)
JOIN PRODUCT p ON p.Product_ID = t.product_id
JOIN CUSTOMER c ON c.Customer_ID = t.customer_id
JOIN STORE s ON s.Store_ID = t.store_id
JOIN SUPPLIER sp ON sp.Supplier_ID = t.supplier_id;



