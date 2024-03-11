package DB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ahmedgado
 */

public class Util {
    public static ArrayList<String[]> getMetadata(String tableName) {
        if (DBApp.db_config == null) {
            throw new RuntimeException("DBApp not initialized");
        }

        ArrayList<String[]> metadata;
        String metadataPath = DBApp.db_config.getProperty("MetadataPath");

        try (BufferedReader br = new BufferedReader(new FileReader(metadataPath))) {
            br.readLine(); // Skip the header
            metadata = br.lines()
                         .map(line -> line.split(","))
                         .filter(line -> tableName == null || line[0].equals(tableName))
                         .collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return metadata;
    }

    public static LinkedList linearSearch(SQLTerm sqlTerm) {
        // TODO: Implement linear search
        ArrayList<String[]> metadata = getMetadata(sqlTerm._strTableName);
        Iterator tableIterator = Table.loadTable(sqlTerm._strTableName).iterator();
        LinkedList result = new LinkedList();

        while (tableIterator.hasNext()) {
            Hashtable record = (Hashtable) tableIterator.next();
            Object value = record.get(sqlTerm._strColumnName);
            if (evaluate(value, sqlTerm._strOperator, sqlTerm._objValue)) {
                result.add(record);
            }
        }

        return result;
    }

    public static boolean evaluate(Object value, Object operator, Object objValue) {
        if (value == null || objValue == null) {
            return false;
        }

        return switch ((String) operator) {
            case "=" -> value.equals(objValue);
            case "!=" -> !value.equals(objValue);
            case ">" -> ((Comparable) value).compareTo(objValue) > 0;
            case ">=" -> ((Comparable) value).compareTo(objValue) >= 0;
            case "<" -> ((Comparable) value).compareTo(objValue) < 0;
            case "<=" -> ((Comparable) value).compareTo(objValue) <= 0;
            default -> throw new RuntimeException("Invalid operator");
        };
    }
}