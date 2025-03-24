import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

public class LoadCSVToDatabase {

    public static void main(String[] args) {
        String csvFilePath = "output.csv";

        try (Scanner scanner = new Scanner(System.in)) {
            // user input for database credentials
            System.out.print("Enter database URL (e.g., jdbc:mysql://localhost:3306/metro_dw): ");
            String jdbcURL = scanner.nextLine();

            System.out.print("Enter database username: ");
            String username = scanner.nextLine();

            System.out.print("Enter database password: ");
            String password = scanner.nextLine();

            String insertQuery = "INSERT INTO output_data (order_id, order_date, product_id, quantity_ordered, " +
                    "customer_id, customer_name, gender, product_name, product_price, store_id, store_name, " +
                    "supplier_id, supplier_name, total_sale) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (Connection connection = DriverManager.getConnection(jdbcURL, username, password);
                    BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {

                // skip header
                String line = reader.readLine();

                PreparedStatement statement = connection.prepareStatement(insertQuery);

                while ((line = reader.readLine()) != null) {
                    List<String> data = parseCSV(line);

                    try {
                        // populate using parsed data
                        statement.setInt(1, Integer.parseInt(data.get(0).trim())); // Order ID
                        statement.setTimestamp(2, Timestamp.valueOf(data.get(1).trim())); // Order Date
                        statement.setInt(3, Integer.parseInt(data.get(2).trim())); // Product ID
                        statement.setInt(4, Integer.parseInt(data.get(3).trim())); // Quantity Ordered
                        statement.setInt(5, Integer.parseInt(data.get(4).trim())); // Customer ID
                        statement.setString(6, data.get(5).trim()); // Customer Name
                        statement.setString(7, data.get(6).trim()); // Gender
                        statement.setString(8, data.get(7).trim()); // Product Name
                        statement.setDouble(9, parseSafely(data.get(8))); // Product Price
                        statement.setInt(10, Integer.parseInt(data.get(9).trim())); // Store ID
                        statement.setString(11, data.get(10).trim()); // Store Name
                        statement.setInt(12, Integer.parseInt(data.get(11).trim())); // Supplier ID
                        statement.setString(13, data.get(12).trim()); // Supplier Name
                        statement.setDouble(14, parseSafely(data.get(13))); // Total Sale

                        statement.addBatch(); // batch for efficiency
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        System.err.println("Skipping invalid row: " + line);
                    }
                }

                statement.executeBatch(); // execute all at once for better performance
                System.out.println("Data has been loaded successfully!");

            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // helper method to safely parse doubles
    private static double parseSafely(String value) {
        try {
            return Double.parseDouble(value.replace("$", "").replace(",", "").trim());
        } catch (NumberFormatException e) {
            System.err.println("Invalid numeric value: " + value + ". Defaulting to 0.0");
            return 0.0; // default value for invalid numbers
        }
    }

    // helper method to parse CSV line correctly, handling commas inside quotes
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
                // add character to the current field
                currentField.append(c);
            }
        }

        fields.add(currentField.toString().trim());
        return fields;
    }
}
