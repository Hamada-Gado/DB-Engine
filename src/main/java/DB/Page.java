package DB;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

/**
 * @author ahmedgado
 */

public class Page implements Serializable {
    private final int max;
    private final Vector<Hashtable<String, Object>> records;

    public Page(int max) {
        this.max = max;
        this.records = new Vector<>();
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
