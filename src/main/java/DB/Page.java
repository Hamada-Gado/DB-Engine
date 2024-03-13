package DB;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

/**
 * @author ahmedgado
 */

public class Page implements Iterable<Hashtable<String, Object>>, Serializable {
    private final int max;
    private final Vector<Hashtable<String, Object>> records;

    public Page(int max) {
        this.max = max;
        this.records = new Vector<>();
    }

    public int getMax(){
        return max;
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


    @NotNull
    @Override
    public Iterator<Hashtable<String, Object>> iterator() {
        return new PageIterator();
    }

    private class PageIterator implements Iterator<Hashtable<String, Object>> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < records.size();
        }

        @Override
        public Hashtable<String, Object> next() {
            if (!hasNext()) {
                throw new RuntimeException("No more records");
            }

            return records.get(index++);
        }
    }
}
