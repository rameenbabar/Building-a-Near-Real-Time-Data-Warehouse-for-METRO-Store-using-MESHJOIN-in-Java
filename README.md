# Overview
This project demonstrates the development of a near-real-time Data Warehouse (DW) prototype for the METRO shopping store in Pakistan, showcasing how transactional data can be processed, enriched, and analyzed using a Java-based implementation of the extended MESHJOIN algorithm.

The objective is to replicate a real-world business intelligence workflow where incoming customer transactions are continuously streamed, enriched with master data (product and customer details), and loaded into a star-schema-based DW designed in MySQL. The enriched data supports advanced OLAP (Online Analytical Processing) queries that provide actionable business insights such as top-selling products, seasonal trends, supplier contributions, and revenue volatility.
# Datasets
The following datasets are used in this project:

- `transactions.csv`: Contains customer purchase records.
- `customers.csv`: Master data with customer details.
- `products.csv`: Master data with product details.
- `output.csv`: Generated file containing enriched transactional data ready to be loaded into the data warehouse.

# Prerequisites

**Java Development Kit (JDK):** Ensure JDK 11 or later is installed.

**MySQL Server:** Ensure MySQL Server is installed and running.

**MySQL Workbench:** For executing database schema creation and OLAP queries.

**MySQL Connector for Java:** Use `mysql-connector-j-9.1.0.jar`.

**Visual Studio Code:** IDE for running the Java code.

# Setup Instructions

## Step 1: Prepare the Environment
Install JDK and set up the JAVA_HOME environment variable.

Install MySQL Server and Workbench.

Download and place the `mysql-connector-j-9.1.0.jar` file in a directory of your choice (e.g., C:\Project).

## Step 2: Configure the Database
Open MySQL Workbench.

Execute the following SQL commands to set up the schema:

Create the output_data table to store enriched transactional data.

Create dimension tables (TIME_DIM, PRODUCT, CUSTOMER, STORE, SUPPLIER) and the SALES fact table using the provided schema.

Verify the tables are created successfully using SHOW TABLES;.

## Step 3: Run the Java Code in VS Code
Open Visual Studio Code.

Install the following extensions:

Java Extension Pack for Java development.

Place the following files in your workspace:

`MeshJoin.java`

`LoadCSVToDatabase.java`

`mysql-connector-j-9.1.0.jar` (in the same directory as your .java files or in a lib folder).

## Step 4: Compile and Run Java Code
Open the terminal in VS Code.

**Compile the MeshJoin program:**

`javac -cp .;C:\Project\mysql-connector-j-9.1.0.jar MeshJoin.java`

**Run the MeshJoin program:**

`java -cp .;C:\Project\mysql-connector-j-9.1.0.jar MeshJoin`

**When prompted, enter the database credentials (URL, username, and password). For example:**

`Enter database URL (e.g., jdbc:mysql://localhost:3306/metro_dw): jdbc:mysql://localhost:3306/databasrName`

`Enter database username: username`

`Enter database password: password`

The program will process transactional data (transactions.csv), enrich it with master data, and export the results to output.csv.

## Step 5: Load Data to the Database
Compile and run the `LoadCSVToDatabase` program:

`javac -cp .;C:\Project\mysql-connector-j-9.1.0.jar LoadCSVToDatabase.java`

`java -cp .;C:\Project\mysql-connector-j-9.1.0.jar LoadCSVToDatabase`

Enter the database credentials as prompted.

The program will load the enriched data from output.csv into the output_data table in the database.

## Step 6: Set Up the Schema in MySQL Workbench
Open MySQL Workbench and connect to your database server.

Create the `metro_dw` star schema and tables using the `create-DW.sql` script

## Step 7: Execute OLAP Queries
Open MySQL Workbench and connect to the database.

Execute the provided OLAP queries to analyze the data. These include insights into revenue trends, product sales, seasonal analysis, and store performance.

## Notes
Ensure the database connection details are accurate when running the Java programs.

Handle large datasets by increasing the heap size if necessary using `-Xmx JVM` options.

Verify data consistency in the output_data table before executing OLAP queries.
