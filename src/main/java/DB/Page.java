package DB;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Vector;

/**
 * @author ahmedgado
 */

public class Page implements Serializable {
    private final String tableName;
    private final int max;
    private final Vector<Hashtable<String, Object>> records;

    public Page(String tableName, int max) {
        this.tableName = tableName;
        this.max = max;
        this.records = new Vector<>();
    }

    public void updatePage() { //this method is used to serialize the page
        Path path = Paths.get((String) DBApp.getDb_config().get("DataPath"), tableName, hashCode() + ".ser");
        try (
                FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isFull() {
        return records.size() == max;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public Vector<Hashtable<String, Object>> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        for (Hashtable<String, Object> record : records) {
            StringBuilder tuple = new StringBuilder();

            for (String key : record.keySet()) {
                tuple.append(record.get(key)).append(",");
            }
            tuple.deleteCharAt(tuple.length() - 1);

            res.append(tuple).append("\n");
        }

        return res.toString();
    }
}
