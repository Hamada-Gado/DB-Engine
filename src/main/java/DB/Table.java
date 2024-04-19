package DB;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.*;

/**
 * This class represents a Table object that implements Iterable, Cloneable, and Serializable interfaces.
 * It contains a list of Page objects and provides methods to manipulate and save these pages.
 *
 * @author ahmedgado
 */
public class Table<PKey> implements Iterable<Page>, Cloneable, Serializable {
    private final String tableName;
    private Vector<String> pagesPath;
    private Vector<Comparable<PKey>> clusteringKeyMin;
    private int lastPageNumber = 0;

    /**
     * Constructor for the Table class.
     *
     * @param tableName The name of the table.
     */
    public Table(String tableName) {
        this.tableName = tableName;
        pagesPath = new Vector<>();
        clusteringKeyMin = new Vector<>();
    }

    /**
     * @return The paths of the pages.
     */
    public Vector<String> getPagesPath() {
        return pagesPath;
    }

    /**
     * @return The minimum values of the clustering keys.
     */
    public Vector<Comparable<PKey>> getClusteringKeyMin() {
        return clusteringKeyMin;
    }

    /**
     * Clears the pagesPath and clusteringKeyMin vectors and resets the lastPageNumber to 0.
     */
    public void clear() {
        pagesPath.clear();
        clusteringKeyMin.clear();
        lastPageNumber = 0;
    }

    /**
     * Serializes the table (not the pages) and saves it to a file.
     */
    public void saveTable() {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, tableName + ".ser");
        try (
                FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new Page object, serializes it, and adds its path to the table.
     *
     * @param max The maximum number of records in a page.
     * @return The created Page object.
     */
    public Page addPage(int max) {
        Page page = new Page(tableName, lastPageNumber++, max);

        page.savePage();

        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, page.getPageNumber() + ".ser");
        pagesPath.add(path.toAbsolutePath().toString());

        saveTable();

        return page;
    }

    /**
     * Removes a page from the table and deletes its file.
     *
     * @param index The index of the page to be removed.
     */
    public void removePage(int index) {
        File file = new File(pagesPath.get(index));
        if (!file.delete()) {
            throw new RuntimeException("Failed to delete the page");
        }

        pagesPath.remove(index);
        clusteringKeyMin.remove(index);
    }

    /**
     * Removes a page from the table and deletes its file.
     *
     * @param page The page to be removed.
     */
    public void removePage(Page page) {
        String pageName = Paths.get((String) DBApp.getDbConfig().get("DataPath"),
                tableName, page.getPageNumber() + ".ser").toAbsolutePath().toString();
        int index = pagesPath.indexOf(pageName);
        removePage(index);
    }

    /**
     * Deserializes a page from a file.
     *
     * @param index The index of the page.
     * @return The deserialized Page object.
     */
    public Page getPage(int index) {
        String path = pagesPath.get(index);

        Page page;
        try (
                FileInputStream fileIn = new FileInputStream(path);
                ObjectInputStream in = new ObjectInputStream(fileIn)) {
            page = (Page) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return page;
    }

    /**
     * @return The number of pages in the table.
     */
    public int pagesCount() {
        return pagesPath.size();
    }

    /**
     * Adds a record to a page and updates the clustering key minimum value.
     *
     * @param record The record to be added.
     * @param pKey   The primary key.
     * @param page   The page to which the record should be added.
     */
    public void addRecord(Record record, String pKey, Page page) {
        page.add(record);
        clusteringKeyMin.add(page.getPageNumber(), (Comparable<PKey>) page.getRecords().getFirst().hashtable().get(pKey));
        saveTable();
    }

    /**
     * Adds a record at a specific position in a page and updates the clustering key minimum value.
     *
     * @param recordNo The position at which the record should be added.
     * @param record   The record to be added.
     * @param pKey     The primary key.
     * @param page     The page to which the record should be added.
     */
    public void addRecord(int recordNo, Record record, String pKey, Page page) {
        page.add(recordNo, record);
        clusteringKeyMin.add(page.getPageNumber(), (Comparable<PKey>) page.getRecords().getFirst().hashtable().get(pKey));
        saveTable();
    }

    /**
     * Removes a record at a specific position from a page and updates the clustering key minimum value.
     *
     * @param recordNo The position of the record to be removed.
     * @param pKey     The primary key.
     * @param page     The page from which the record should be removed.
     * @return The removed record.
     */
    public Record removeRecord(int recordNo, String pKey, Page page) {
        Record htbl = page.remove(recordNo);
        if (page.isEmpty()) {
            String pageName = Paths.get((String) DBApp.getDbConfig().get("DataPath"),
                    tableName, page.getPageNumber() + ".ser").toAbsolutePath().toString();
            int index = pagesPath.indexOf(pageName);
            pagesPath.remove(index);
            clusteringKeyMin.remove(index);
        } else {
            clusteringKeyMin.add(page.getPageNumber(), (Comparable<PKey>) page.getRecords().getFirst().hashtable().get(pKey));
        }
        saveTable();

        return htbl;
    }

    /**
     * This method overrides the toString method from the Object class.
     * It iterates over the pages and appends each page to a StringBuilder.
     * If the StringBuilder is not empty, it removes the last newline character.
     *
     * @return A string representation of the pages.
     */
    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < pagesCount(); i++) {
            Page page = getPage(i);
            res.append("Page ")
                    .append(i)
                    .append(":\n")
                    .append(page)
                    .append("\n");
        }
        if (pagesCount() > 0) {
            res.deleteCharAt(res.length() - 1);
        }

        return res.toString();
    }

    /**
     * Deserializes a table from a file.
     *
     * @param tableName The name of the table.
     * @return The deserialized Table object.
     * @throws DBAppException If the table does not exist.
     */
    public static <T> Table<T> loadTable(String tableName) throws DBAppException {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, tableName + ".ser");

        if (!path.toFile().exists()) {
            throw new DBAppException("Table doesn't exit");
        }

        Table<T> table;
        try (
                FileInputStream fileIn = new FileInputStream(path.toAbsolutePath().toString());
                ObjectInputStream in = new ObjectInputStream(fileIn)) {
            table = (Table<T>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    /**
     * @return An iterator over the pages in the table.
     */
    public @NotNull Iterator<Page> iterator() {
        return new TableIterator();
    }

    /**
     * Creates a copy of the table.
     *
     * @return The copied Table object.
     */
    @Override
    public Table<PKey> clone() {
        try {
            Table<PKey> clone = (Table<PKey>) super.clone();
            clone.pagesPath = (Vector<String>) pagesPath.clone();
            clone.clusteringKeyMin = (Vector<Comparable<PKey>>) clusteringKeyMin.clone();

            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    /**
     * This class represents an iterator over the pages in the table.
     */
    private class TableIterator implements Iterator<Page> {
        private int pageIndex;
        private Page page;

        /**
         * Constructor for the TableIterator class.
         */
        public TableIterator() {
            pageIndex = 0;
            page = null;
        }

        /**
         * @return True if there are more pages, false otherwise.
         */
        @Override
        public boolean hasNext() {
            return pageIndex < pagesCount();
        }

        /**
         * @return The next page.
         * @throws RuntimeException If there are no more pages.
         */
        @Override
        public Page next() {
            if (!hasNext()) {
                throw new RuntimeException("No more records");
            }

            page = getPage(pageIndex);
            pageIndex++;

            return page;
        }
    }
}