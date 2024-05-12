package DB;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Vector;

/**
 * This class represents a Page object that implements Serializable interface.
 * It contains a list of Record objects and provides methods to manipulate and save these records.
 */
public class Page implements Serializable {
    private final String tableName;
    private final int pageNumber;
    private final int max;
    public Vector<Record> records;

    /**
     * Constructor for the Page class.
     *
     * @param tableName  The name of the table.
     * @param pageNumber The page number.
     * @param max        The maximum number of records that can be stored in the page.
     */
    public Page(String tableName, int pageNumber, int max) {
        this.tableName = tableName;
        this.pageNumber = pageNumber;
        this.max = max;
        this.records = new Vector<>();
    }

    /**
     * This method saves the current page to a file.
     */
    public void savePage() {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, pageNumber + ".ser");
        try (
                FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return The size of the records vector.
     */
    public int size() {
        return records.size();
    }

    /**
     * @return True if the records vector is empty, false otherwise.
     */
    public boolean isEmpty() {
        return records.isEmpty();
    }

    /**
     * @return The page number.
     */
    public int getPageNumber() {
        return pageNumber;
    }

    /**
     * @return The maximum number of records that can be stored in the page.
     */
    public int getMax() {
        return max;
    }

    /**
     * @return The records vector.
     */
    public Vector<Record> getRecords() {
        return records;
    }

    /**
     * Sets the records vector.
     *
     * @param records The new records vector.
     */
    public void setRecords(Vector<Record> records) {
        this.records = records;
    }

    /**
     * Adds a record to the records vector and saves the page.
     *
     * @param record The record to be added.
     */
    public void add(Record record) {
        records.add(record);
        savePage();
    }

    /**
     * Adds a record at a specific position in the records vector and saves the page.
     *
     * @param recordNo The position at which the record should be added.
     * @param record   The record to be added.
     */
    public void add(int recordNo, Record record) {
        records.add(recordNo, record);
        savePage();
    }

    /**
     * Removes a record at a specific position in the records vector and saves the page.
     *
     * @param recordNo The position of the record to be removed.
     * @return The removed record.
     */
    public Record remove(int recordNo) {
        Record htbl = records.remove(recordNo);
        savePage();
        return htbl;
    }

    /**
     * This method overrides the toString method from the Object class.
     * It iterates over the records vector and appends each record to a StringBuilder.
     * If the StringBuilder is not empty, it removes the last newline character.
     *
     * @return A string representation of the records vector.
     */
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