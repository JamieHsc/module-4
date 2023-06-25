import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FileDatabaseManager {

    private static final String DATABASE_NAME = "file_database";
    private static final String DATABASE_URL = "jdbc:derby:" + DATABASE_NAME + ";create=true";

    public static void main(String[] args) {
        try {
            // Create the database tables if they don't exist
            createTablesIfNotExist();

            // Take input directory from the user
            String directory = getInputDirectory();

            // Insert files into the database
            insertFiles(directory);

            // Display all tables in the database
            displayTables();

            // Display files from the chosen directory
            displayFilesFromDirectory(directory);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void createTablesIfNotExist() throws SQLException {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS files (" +
                    "id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY," +
                    "file_name VARCHAR(255)," +
                    "path VARCHAR(255)," +
                    "extension VARCHAR(255)," +
                    "size BIGINT" +
                    ")");
        }
    }

    private static String getInputDirectory() throws IOException {
        // TODO: Implement code to take input directory from the user
        // For simplicity, I'm directly returning a sample directory here
        return "C:/sample_directory";
    }

    private static void insertFiles(String directory) throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL);
             Statement statement = connection.createStatement()) {

            // Retrieve every file and subfile within the directory
            File rootDir = new File(directory);
            if (rootDir.exists() && rootDir.isDirectory()) {
                Path rootPath = Paths.get(directory);
                Files.walk(rootPath)
                        .filter(Files::isRegularFile)
                        .forEach(file -> {
                            try {
                                String fileName = file.getFileName().toString();
                                String path = rootPath.relativize(file).toString();
                                String extension = FileUtils.getExtension(fileName);
                                long size = Files.size(file);

                                // Insert file details into the database
                                String insertQuery = String.format("INSERT INTO files (file_name, path, extension, size) " +
                                        "VALUES ('%s', '%s', '%s', %d)", fileName, path, extension, size);
                                statement.executeUpdate(insertQuery);
                            } catch (IOException | SQLException e) {
                                e.printStackTrace();
                            }
                        });
            }
        }
    }

    private static void displayTables() throws SQLException {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE='T'");
            System.out.println("Tables in the database:");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("TABLENAME"));
            }
        }
    }

    private static void displayFilesFromDirectory(String directory) throws SQLException {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM files");
            System.out.println("Files from the chosen directory:");
            System.out.println("----------------------------------------------------");
            System.out.printf("%-30s%-50s%-15s%-10s%n", "File Name", "Path", "Extension", "Size (Bytes)");
            System.out.println("----------------------------------------------------");
            while (resultSet.next()) {
                String fileName = resultSet.getString("file_name");
                String path = resultSet.getString("path");
                String extension = resultSet.getString("extension");
                long size = resultSet.getLong("size");
                System.out.printf("%-30s%-50s%-15s%-10d%n", fileName, path, extension, size);
            }
        }
    }
}