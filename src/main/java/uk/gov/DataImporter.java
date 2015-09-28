package uk.gov;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.codec.binary.Hex;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingIterator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.postgresql.util.PGobject;

import java.io.*;
import java.security.MessageDigest;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class DataImporter {
    private static final String url = "jdbc:postgresql://localhost:5432/domains";
    static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException, SQLException {
        recreateTable();

        loadEntries("domains_list_2011.csv", LocalDate.of(2011, 10, 21).atStartOfDay());

        loadEntries("domains_list_2012.csv", LocalDate.of(2012, 10, 1).atStartOfDay());

        loadEntries("domains_list_2013.csv", LocalDate.of(2013, 10, 1).atStartOfDay());

        loadEntries("domains_list_2014.csv", LocalDate.of(2014, 10, 1).atStartOfDay());

        createFile("data/domains/domains.txt");
    }

    private static void recreateTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE IF EXISTS DOMAINS");
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS DOMAINS (ID SERIAL PRIMARY KEY, ENTRY JSONB)");
            }
        }
    }

    private static void createFile(String filePath) throws IOException, SQLException {
        File file = new File(filePath);
        //noinspection ResultOfMethodCallIgnored
        file.createNewFile();
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT ENTRY FROM DOMAINS ORDER BY ID");

                while (resultSet.next()) {
                    bw.write(new Record(resultSet.getString("ENTRY")).entry.toString() + "\n");
                }
            }
        }
        bw.close();
    }

    protected static void loadEntries(String fileName, LocalDateTime fileTime) throws IOException, SQLException {
        final Collection<Record> recordsFromDB = fetchCurrentRecordsFromDB();

        final List<Record> recordsFromFile = loadFile(fileName);
        final List<Record> domainsToBeRemovedFromDB = recordsFromDB.stream()
                .filter(r -> !recordsFromFile.stream()
                                .map(Record::domainName)
                                .collect(Collectors.toList())
                                .contains(r.domainName())
                ).filter(r ->
                                r.entry.get("end-date").getTextValue().isEmpty()
                )
                .collect(Collectors.toList());
        insertDomains(domainsToBeRemovedFromDB, true, fileTime);

        final List<Record> recordsToBeInsertedOrUpdatedInDB = recordsFromFile.stream()
                .filter(r -> !recordsFromDB.stream()
                                .map(m -> m.hash)
                                .collect(Collectors.toList())
                                .contains(r.hash)
                )
                .collect(Collectors.toList());

        insertDomains(recordsToBeInsertedOrUpdatedInDB, false, fileTime);
    }

    protected static Collection<Record> fetchCurrentRecordsFromDB() throws SQLException, IOException {
        List<Record> recordsFromDB = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT ENTRY FROM DOMAINS ORDER BY ID");

                while (resultSet.next()) {
                    recordsFromDB.add(new Record(resultSet.getString("ENTRY")));
                }
            }
        }

        return current(recordsFromDB);
    }

    private static Collection<Record> current(List<Record> recordsFromDB) {
        Map<String, Record> recordMap = new HashMap<>();
        for (Record record : recordsFromDB) {
            recordMap.put(record.domainName(), record);
        }
        return recordMap.values();
    }

    private static void insertDomains(List<Record> records, boolean isDeleted, LocalDateTime fileTime) throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection(url)) {
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO DOMAINS(ENTRY) VALUES(?)")) {
                for (Record record : records) {
                    statement.setObject(1, (isDeleted ? record.withEndDate(fileTime) : record).getPGObject());
                    statement.executeUpdate();
                }
            }
        }
    }

    static class Record {
        String hash;
        JsonNode entry;

        public Record(String data) throws IOException {
            JsonNode jsonNode = objectMapper.readTree(data);
            this.hash = jsonNode.get("hash").getTextValue();
            this.entry = jsonNode.get("entry");
        }

        public Record(JsonNode entry) {
            this.hash = shasum(entry.toString());
            this.entry = entry;
        }

        private PGobject createPGObject(String data) {
            PGobject pgo = new PGobject();
            pgo.setType("jsonb");
            try {
                pgo.setValue(data);
            } catch (Exception e) { //success: api setter throws checked exception
            }
            return pgo;
        }

        public PGobject getPGObject() throws IOException {
            ObjectNode jsonNodes = new ObjectNode(JsonNodeFactory.instance);
            jsonNodes.put("hash", hash);
            jsonNodes.put("entry", entry);
            return createPGObject(jsonNodes.toString());
        }

        public String domainName() {
            return entry.get("government-domain").getTextValue();
        }

        public Record withEndDate(LocalDateTime fileTime) {
            @SuppressWarnings("unchecked") Map<String, Object> map = objectMapper.convertValue(entry, Map.class);
            map.put("end-date", fileTime.toString());
            JsonNode entryNode = objectMapper.convertValue(map, JsonNode.class);
            return new Record(entryNode);
        }
    }

    protected static List<Record> loadFile(String fileName) throws IOException, SQLException {
        CsvSchema.Builder builder = CsvSchema.builder().setColumnSeparator(',').setUseHeader(true);
        builder.addColumn("government-domain");
        builder.addColumn("owner");
        CsvSchema csvSchema = builder.build();

        InputStream inputStream = DataImporter.class.getClassLoader().getResourceAsStream(fileName);

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

        MappingIterator<Map<String, Object>> entries = new CsvMapper().reader(Map.class).withSchema(csvSchema).readValues(br);
        List<Record> records = new ArrayList<>();
        while (entries.hasNext()) {
            records.add(createRecord(entries.next()));
        }
        return records;

    }


    protected static Record createRecord(Map<String, Object> entry) {
        entry.put("end-date", "");
        JsonNode entryNode = objectMapper.convertValue(entry, JsonNode.class);

        return new Record(entryNode);
    }


    static String shasum(String raw) {
        try {
            String head = "blob " + raw.getBytes("UTF-8").length + "\0";

            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update((head + raw).getBytes("UTF-8"));
            return new String(Hex.encodeHex(md.digest()));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
