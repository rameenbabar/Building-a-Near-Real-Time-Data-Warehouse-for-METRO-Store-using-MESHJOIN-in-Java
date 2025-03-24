import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class MeshJoin {

    private static final BlockingQueue<List<Transaction>> transactionQueue = new LinkedBlockingQueue<>(10); // buffer
    private static final Map<String, List<String>> customerPartitions = new HashMap<>();
    private static final Map<String, List<String>> productPartitions = new HashMap<>();
    private static final HashMap<Integer, Result> resultHashTable = new HashMap<>(); // hashtable for results
    private static final int CHUNK_SIZE = 100; // transactions per chunk

    private static final String DB_URL = "jdbc:mysql://localhost:3306/metro_dw";
    private static final String DB_USER = "username";
    private static final String DB_PASSWORD = "password";

    public static void main(String[] args) {

        try (Scanner scanner = new Scanner(System.in)) {
            // user input for database credentials
            System.out.print("Enter database URL (e.g., jdbc:mysql://localhost:3306/metro_dw): ");
            String dbUrl = scanner.nextLine();

            System.out.print("Enter database username: ");
            String dbUser = scanner.nextLine();

            System.out.print("Enter database password: ");
            String dbPassword = scanner.nextLine();

            loadPartitionsFromDatabase("customers", customerPartitions, "C");
            loadPartitionsFromDatabase("products", productPartitions, "P");

            // begin producer thread
            Thread producer = new Thread(() -> {
                try {
                    loadTransactionsInChunks("transactions.csv");
                } catch (IOException e) {
                    System.err.println("Producer error: " + e.getMessage());
                }
            });

            // begin consumer thread
            Thread consumer = new Thread(() -> {
                try {
                    executeMeshJoin();
                } catch (InterruptedException e) {
                    System.err.println("Consumer error: " + e.getMessage());
                }
            });

            producer.start();
            consumer.start();

            producer.join();
            consumer.join();

            // store results to a csv
            storeResults("output.csv");
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    // store the results to CSV
    private static void storeResults(String outputFilePath) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {
            // header row
            bw.write(
                    "Order ID,Order Date,Product ID,Quantity Ordered,Customer ID,Customer Name,Gender,Product Name,Product Price,Store ID,Store Name,Supplier ID,Supplier Name,Total Sale\n");

            // result row
            for (Result result : resultHashTable.values()) {
                bw.write(result.toCSV());
                bw.newLine(); // each record is on a new line
            }
        } catch (IOException e) {
            System.err.println("Error while exporting results: " + e.getMessage());
        }
        System.out.println("Results saved to " + outputFilePath);
    }

    // load partitions from the database
    private static void loadPartitionsFromDatabase(String tableName, Map<String, List<String>> partitions,
            String prefix) {
        String query = tableName.equals("customers")
                ? "SELECT customer_id, customer_name, gender FROM customers"
                : "SELECT productID, productName, productPrice, supplierID, supplierName, storeID, storeName FROM products";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {

            int partitionNum = 1;
            while (resultSet.next()) {
                StringBuilder row = new StringBuilder();
                if (tableName.equals("customers")) {
                    row.append(resultSet.getInt("customer_id")).append(",");
                    row.append(quotecheck(resultSet.getString("customer_name"))).append(",");
                    row.append(resultSet.getString("gender"));
                } else if (tableName.equals("products")) {
                    row.append(resultSet.getInt("productID")).append(",");
                    row.append(quotecheck(resultSet.getString("productName"))).append(",");
                    row.append(resultSet.getBigDecimal("productPrice")).append(",");
                    row.append(resultSet.getInt("supplierID")).append(",");
                    row.append(quotecheck(resultSet.getString("supplierName"))).append(",");
                    row.append(resultSet.getInt("storeID")).append(",");
                    row.append(quotecheck(resultSet.getString("storeName")));
                }

                String partitionKey = prefix + partitionNum;
                partitions.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row.toString());
                partitionNum = partitionNum % 3 + 1; // Cycle through partitions
            }
        } catch (SQLException e) {
            System.err.println("Database error while loading partitions: " + e.getMessage());
        }
    }

    // helper method to escape fields with commas or quotes
    private static String quotecheck(String field) {
        if (field.contains(",") || field.contains("\"")) {
            return "\"" + field.replace("\"", "\"\"") + "\""; // Escape internal quotes
        }
        return field;
    }

    // load transactions in chunks
    private static void loadTransactionsInChunks(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            List<Transaction> chunk = new ArrayList<>();

            while ((line = br.readLine()) != null) {
                if (!line.startsWith("Order ID")) {
                    List<String> parts = parseCSV(line); // parse each transaction row
                    chunk.add(new Transaction(
                            Integer.parseInt(parts.get(0)),
                            parts.get(1),
                            parts.get(4),
                            parts.get(2),
                            Integer.parseInt(parts.get(3))));

                    // when the chunk size is reached add it to the queue
                    if (chunk.size() >= CHUNK_SIZE) {
                        transactionQueue.put(new ArrayList<>(chunk));
                        chunk.clear();
                    }
                }
            }

            // add final chunk if it's not empty
            if (!chunk.isEmpty()) {
                transactionQueue.put(chunk);
            }

            // termination signal - null chunk to stop the consumer
            transactionQueue.put(Collections.emptyList());
        } catch (InterruptedException e) {
            System.err.println("Producer interrupted: " + e.getMessage());
        }
    }

    // implemet the mesh join
    private static void executeMeshJoin() throws InterruptedException {
        while (true) {
            List<Transaction> chunk = transactionQueue.take(); // retrieve a chunk from the queue

            // check termination signal
            if (chunk.isEmpty()) {
                System.out.println("Consumer received termination signal. Stopping...");
                break;
            }

            for (Transaction transaction : chunk) {
                processTransaction(transaction);
            }
        }
    }

    // process a transaction and perform the mesh join
    private static void processTransaction(Transaction transaction) {
        for (List<String> customerRecords : customerPartitions.values()) {
            for (String customerRecord : customerRecords) {
                List<String> customerData = parseCSV(customerRecord); // Parse customer record
                if (customerData.get(0).equals(transaction.customerId)) {
                    processCustomerRecord(transaction, customerData);
                    return;
                }
            }
        }
    }

    // process customer record and find matching product
    private static void processCustomerRecord(Transaction transaction, List<String> customerData) {
        for (List<String> productRecords : productPartitions.values()) {
            for (String productRecord : productRecords) {
                List<String> productData = parseCSV(productRecord); // Parse product record
                if (productData.get(0).equals(transaction.productId)) {
                    double totalSale = transaction.quantityOrdered * parseSafely(productData.get(2));
                    Result result = new Result(
                            transaction, customerData, productData, productData.get(5), productData.get(6),
                            productData.get(3), productData.get(4), totalSale);
                    resultHashTable.put(transaction.orderId, result); // Store result in hash table
                    return;
                }
            }
        }
    }

    // helper method to safely parse a double
    private static double parseSafely(String value) {
        try {
            return Double.parseDouble(value.replace("$", "").replace(",", ""));
        } catch (NumberFormatException e) {
            System.err.println("Invalid numeric value: " + value + ". Defaulting to 0.0");
            return 0.0; // Default value for invalid numbers
        }
    }

    // helper method to parse CSV line correctly, handling commas and quotes
    private static List<String> parseCSV(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // handle escaped quotes
                    currentField.append(c);
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString().trim());
                currentField.setLength(0);
            } else {
                currentField.append(c);
            }
        }

        fields.add(currentField.toString().trim());
        return fields;
    }

    // static class to represent a transaction
    static class Transaction {
        int orderId;
        String orderDate;
        String customerId;
        String productId;
        int quantityOrdered;

        public Transaction(int orderId, String orderDate, String customerId, String productId, int quantityOrdered) {
            this.orderId = orderId;
            this.orderDate = orderDate;
            this.customerId = customerId;
            this.productId = productId;
            this.quantityOrdered = quantityOrdered;
        }
    }

    // static class to represent a result
    static class Result {
        Transaction transaction;
        List<String> customerData;
        List<String> productData;
        String storeId;
        String storeName;
        String supplierId;
        String supplierName;
        double totalSale;

        public Result(Transaction transaction, List<String> customerData, List<String> productData,
                String storeId, String storeName, String supplierId, String supplierName, double totalSale) {
            this.transaction = transaction;
            this.customerData = customerData;
            this.productData = productData;
            this.storeId = storeId;
            this.storeName = storeName;
            this.supplierId = supplierId;
            this.supplierName = supplierName;
            this.totalSale = totalSale;
        }

        public String toCSV() {
            return String.join(",",
                    quotecheck(String.valueOf(transaction.orderId)),
                    quotecheck(transaction.orderDate),
                    quotecheck(transaction.productId),
                    String.valueOf(transaction.quantityOrdered),
                    quotecheck(transaction.customerId),
                    quotecheck(customerData.get(1)),
                    quotecheck(customerData.get(2)),
                    quotecheck(productData.get(1)),
                    quotecheck(productData.get(2)),
                    quotecheck(storeId),
                    quotecheck(storeName),
                    quotecheck(supplierId),
                    quotecheck(supplierName),
                    String.valueOf(totalSale));
        }

    }
}
