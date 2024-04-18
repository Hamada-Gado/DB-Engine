package DB;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Vector;

/**
 * @author ahmedgado
 */

public class Page implements Serializable {
    private final String tableName;
    private final int pageNumber;
    private final int max;
    public Vector<Record> records;

    public Page(String tableName, int pageNumber, int max) {
        this.tableName = tableName;
        this.pageNumber = pageNumber;
        this.max = max;
        this.records = new Vector<>();
    }

    public void updatePage() {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, pageNumber + ".ser");
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

    public int size() {
        return records.size();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getMax() {
        return max;
    }

    public Vector<Record> getRecords() {
        return records;
    }

    public void add(Record record) {
        records.add(record);
        updatePage();
    }

    public void add(int recordNo, Record record) {
        records.add(recordNo, record);
        updatePage();
    }

    public Record remove(int recordNo) {
        Record htbl = records.remove(recordNo);
        updatePage();
        return htbl;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        for (Record record : records) {
            res.append(record).append("\n");
        }

        if (!res.isEmpty()) {
            res.deleteCharAt(res.length() - 1);
        }

        return res.toString();
    }
}
