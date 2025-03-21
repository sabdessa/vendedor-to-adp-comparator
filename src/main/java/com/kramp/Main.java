package com.kramp;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class Main {

    public static final int CHUNK_SIZE = 50_000_000;
    public static final long THIRTY_DAYS_MILLIS = 30L * 1000 * 60 * 60 * 24;
    static AtomicInteger similarities = new AtomicInteger(0);
    private static final Object connectionLock = new Object();


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Type environment (int, stag, prod): ");
        System.out.println("Default is int");
        String environment = scanner.nextLine().toLowerCase();
        if (environment.isEmpty()) {
            System.out.println("No environment provided. Defaulting to int");
            environment = "int";
        }
        if (!environment.equals("int") && !environment.equals("stag") && !environment.equals("prod")) {
            System.out.println("Invalid environment. Exiting...");
            return;
        }
        EnvConfig envConfig = loadEnvConfig(environment);

        String adpFilePath = environment + "-" + "adp-records.csv";
        boolean copyAdpData = shouldCopyData(adpFilePath, scanner, "adp-db");

        String vendedorFilePath = environment + "-" + "vendedor-records.csv";
        boolean copyVendedorData = shouldCopyData(vendedorFilePath, scanner, "vendedor-db");

        String adpPassword = null;
        String vendedorPassword = null;
        if (copyAdpData) {
            System.out.println("Copying data from Data source, Type adp-db password:");
            adpPassword = scanner.nextLine();
        }

        if (copyVendedorData) {
            System.out.println("Copying data from Data source, Type vendedor-db password:");
            vendedorPassword = scanner.nextLine();
        }
        long startTime = System.currentTimeMillis();
        final String finalAdpPassword = adpPassword;
        CompletableFuture<Void> adpCopyTask = CompletableFuture.runAsync(() -> {
            if (copyAdpData) {
                String adpConnectionConfig = "jdbc:postgresql://google/adp?cloudSqlInstance=" + envConfig.getProject() + ":europe-west1:adp-db&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=" + envConfig.getAdpUser() + "&password=" + finalAdpPassword;
                String adpQuery = "SELECT id, is_active   FROM  public.offer ";
                writeRecordsToCsv(adpQuery, adpFilePath, adpConnectionConfig);
            }
        });

        final String finalVendedorPassword = vendedorPassword;
        CompletableFuture<Void> vendedorCopyTask = CompletableFuture.runAsync(() -> {
            if (copyVendedorData) {
                String vendedorConnectionUrl = "jdbc:postgresql://google/vendedor?cloudSqlInstance=" + envConfig.getProject() + ":europe-west1:vendedor-db&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=" + envConfig.getVendedorUser() + "&password=" + finalVendedorPassword;
                String vendedorQuery = "SELECT  CONCAT(item_id,'#',seller_id) as adp_id, is_active FROM  public.offer ";
                writeRecordsToCsv(vendedorQuery, vendedorFilePath, vendedorConnectionUrl);
            }
        });

        CompletableFuture<Void> combinedTask = CompletableFuture.allOf(adpCopyTask, vendedorCopyTask);
        combinedTask.join();

        initalizeDifferencesFile();

        ConcurrentHashMap<String, String> diffs = new ConcurrentHashMap<>();
        compareCsvFiles(adpFilePath, vendedorFilePath, diffs);

        removeDuplicateEntriesFromDiffs(vendedorFilePath, diffs);

        writeEntriesIntoFile(diffs, "differences.csv");

        System.out.println("Execution time: " + (System.currentTimeMillis() - startTime) + "ms");
        System.out.println("Similarities: " + similarities);
        System.out.println("Differences: " + diffs.size());
    }

    private static void removeDuplicateEntriesFromDiffs(String vendedorFilePath, ConcurrentHashMap<String, String> diffs) {

        readFileInParallel(vendedorFilePath, (adpId, rowHash) -> {
            String diff = diffs.get(adpId);
            if (diff != null && !diff.equals(rowHash)) {
                diffs.remove(adpId);
            }
        });
    }

    private static boolean shouldCopyData(String vendedorFilePath, Scanner scanner, String dbName) {
        Path path = Path.of(vendedorFilePath);
        boolean copyVendedorData;
        if (Files.exists(path)) {
            //TODO better to store/read files from bucket and check last modified time based on generation time
            long lastModified = System.currentTimeMillis() - new File(vendedorFilePath).lastModified();
            if (lastModified < THIRTY_DAYS_MILLIS) {
                System.out.printf("Last copied data from %s was less than %d  days ago. Do you want to copy again? (y/n)", dbName, lastModified / 1000 / 60 / 60 / 24);
                copyVendedorData = scanner.nextLine().equals("y");
            } else {
                System.out.printf("Last copied data from %s was more than 30 days ago.", dbName);
                copyVendedorData = true;
            }
        } else {
            copyVendedorData = true;
        }
        return copyVendedorData;
    }

    private static EnvConfig loadEnvConfig(String environment) {
        String fileName = "config-" + environment + ".properties";

        Properties properties = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(input);
        } catch (IOException e) {
            System.out.println("Error loading env.properties file: " + e.getMessage());
        }
        return new EnvConfig(properties.getProperty("adp.user"), properties.getProperty("vendedor.user"), properties.getProperty("project"));
    }

    private static void initalizeDifferencesFile() {
        try (FileWriter fw = new FileWriter("differences.csv", false)) {
        } catch (IOException e) {
            System.out.println("Error writing differences to file: " + e.getMessage());
        }
    }

    private static void writeEntriesIntoFile(Map<String, String> entries, String fileName) {
        long startTime = System.currentTimeMillis();
        try (BufferedWriter br = new BufferedWriter(new FileWriter(fileName))) {
            for (String key : entries.keySet()) {
                br.write(key + "," + entries.get(key));
                br.newLine();
            }
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
        }
        System.out.printf("Writing to file %s took: %d ms%n", fileName, System.currentTimeMillis() - startTime);
    }

    private static void compareCsvFiles(String sourceFile, String targetFile, ConcurrentHashMap<String, String> diffs) {
        System.out.println("Comparing files...");
        long startTime = System.currentTimeMillis();
        try (BufferedReader br = new BufferedReader(new FileReader(sourceFile))) {
            HashMap<String, String> adpRecordsPart = new HashMap<>();
            while (loadNextCsvChunkToMemory(br, adpRecordsPart)) {
                findRecordsInFile(targetFile, adpRecordsPart, diffs);
                adpRecordsPart = new HashMap<>();
            }
        } catch (IOException e) {
            System.out.println("Error reading ADP CSV file: " + e.getMessage());
        }
        System.out.println("Comparing files took: " + (System.currentTimeMillis() - startTime) + "ms");
    }

    private static void findRecordsInFile(String fileName, HashMap<String, String> adpRecords, ConcurrentHashMap<String, String> diffs) {
        BiConsumer<String, String> keyValueConsumer = (adpId, rowHash) -> {
            String value = adpRecords.get(adpId);
            if (value != null) {
                if (!value.equals(rowHash)) {
                    diffs.put(adpId, rowHash);
                } else {
                    similarities.incrementAndGet();
                }
            }
        };
        readFileInParallel(fileName, keyValueConsumer);
    }

    private static void readFileInParallel(String fileName, BiConsumer<String, String> keyValueConsumer) {
        int nThreads = Runtime.getRuntime().availableProcessors();

        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), java.nio.file.StandardOpenOption.READ);) {
            long fileSize = fileChannel.size();
            List<Thread> threads = new ArrayList<>();
            long chunkSize = fileSize / nThreads;

            long start = 0;
            for (int i = 0; i < nThreads; i++) {
                long end;
                if (i == nThreads - 1) {
                    end = fileSize;
                } else {
                    end = findChunkEnd(fileName, start, chunkSize, fileSize);
                }
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, end - start);
                FileChunkConsumer fileChunkConsumer = new FileChunkConsumer(buffer, keyValueConsumer);
                Thread fileChunkReaderThread = Thread.startVirtualThread(fileChunkConsumer);
                threads.add(fileChunkReaderThread);
                start = end + 1;
            }

            for (Thread thread : threads) {
                thread.join();
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Error reading Vendedor CSV file: " + e.getMessage());
        }
    }

    private static boolean loadNextCsvChunkToMemory(BufferedReader br, HashMap<String, String> adpRecords) {

        String line = null;
        try {
            int lineCount = 0;
            while ((line = br.readLine()) != null && lineCount < CHUNK_SIZE) {
                int indexSeparator = line.indexOf(",");

                adpRecords.put(line.substring(0, indexSeparator), line.substring(indexSeparator + 1));
                lineCount++;
            }
        } catch (IOException e) {
            System.out.println("Error reading ADP CSV file: " + e.getMessage());
        }
        if (line != null) {
            int indexSeparator = line.indexOf(",");
            adpRecords.put(line.substring(0, indexSeparator), line.substring(indexSeparator + 1));
        }
        return !adpRecords.isEmpty();
    }

    private static void writeRecordsToCsv(String query, String csvFilePath, String jdbcUrl) {
        long startTime = System.currentTimeMillis();
        try (Connection connection = getSynchronizedConnection(jdbcUrl)) {
            CopyManager copyManager = new CopyManager((BaseConnection) connection);

            try (BufferedWriter br = new BufferedWriter(new FileWriter(csvFilePath))) {
                String sql = """
                         COPY (
                        """ + query + """
                             )
                         TO  stdout DELIMITER ',' CSV
                        """;
                copyManager.copyOut(sql, br);
                System.out.println("Data has been copied from the table to CSV successfully.");
            } catch (Exception e) {
                System.out.println("Error reading CSV file: " + e.getMessage());
            }
        } catch (SQLException e) {
            System.out.println("Connection failure: " + e.getMessage());
        }
        System.out.println("Copying data to CSV took: " + (System.currentTimeMillis() - startTime) + "ms");
    }

    private static long findChunkEnd(String fileName, long chunkStart, long chunkSize, long fileSize) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            long chunkEnd = Math.min(chunkStart + chunkSize, fileSize);
            raf.seek(chunkEnd);
            while (chunkEnd < fileSize) {
                char c = (char) raf.read();
                if (c == '\n' || c == '\r') {
                    break;
                }
                chunkEnd++;
            }
            return chunkEnd;
        }
    }

    private static Connection getSynchronizedConnection(String jdbcUrl) throws SQLException {
        synchronized (connectionLock) {
            return DriverManager.getConnection(jdbcUrl);
        }
    }
}