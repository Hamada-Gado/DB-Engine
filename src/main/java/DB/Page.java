package DB;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Vector;

public class Page implements Serializable {
    private int max;
    private Vector<HashMap<String, Object>> records;

    public Page(int max, int min) {
        this.max = max;
        this.records = new Vector<HashMap<String, Object>>();
    }

    public boolean isFull() {
        return records.size() == max;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public String toString() {
        StringBuilder res = new StringBuilder();

        for (HashMap<String, Object> record : records) {
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
