package DB;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.*;

/**
 * @author ahmedgado
 */

public class Table implements Iterable<Page>, Cloneable, Serializable {
    private final String tableName;
    private Vector<String> pagesPath;
    private Vector<Comparable> clusteringKeyMin;
    private int lastPageNumber = 0;

    public Table(String tableName) {
        this.tableName = tableName;
        pagesPath = new Vector<>();
        clusteringKeyMin = new Vector<>();
    }

    public Vector<String> getPagesPath() {
        return pagesPath;
    }

    public Vector<Comparable> getClusteringKeyMin() {
        return clusteringKeyMin;
    }

    public void clear() {
        pagesPath.clear();
        clusteringKeyMin.clear();
        lastPageNumber = 0;
    }

    /**
     * Serialize the table only not the pages
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
     * @param max the maximum number of records in a page
     *            <p>
     *            serialize the page and add its path to the table
     */


    public Page addPage(int max) {
        Page page = new Page(tableName, lastPageNumber++, max);

        page.savePage();

        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, page.getPageNumber() + ".ser");
        pagesPath.add(path.toAbsolutePath().toString());

        saveTable();

        return page;
    }


    public void removePage(int index) {
        File file = new File(pagesPath.get(index));
        if (!file.delete()) {
            throw new RuntimeException("Failed to delete the page");
        }

        pagesPath.remove(index);
        clusteringKeyMin.remove(index);
    }


    public void removePage(Page page) {
        String pageName = Paths.get((String) DBApp.getDbConfig().get("DataPath"),
                tableName, page.getPageNumber() + ".ser").toAbsolutePath().toString();
        int index = pagesPath.indexOf(pageName);
        removePage(index);
    }

    /**
     * @param index the index of the page
     * @return the name of the table
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
     * @param pageName the name of the page
     * @return the page
     */
    public Page getPage(String pageName) {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, pageName + ".ser");
        int index = pagesPath.indexOf(path.toAbsolutePath().toString());
        if (index == -1) {
            throw new RuntimeException("Page not found");
        } else {
            return getPage(index);
        }
    }

    public int pagesCount() {
        return pagesPath.size();
    }

    public void addRecord(Record record, String pKey, Page page) {
        page.add(record);
        clusteringKeyMin.add(page.getPageNumber(), (Comparable) page.getRecords().getFirst().hashtable().get(pKey));
        saveTable();
    }

    public void addRecord(int recordNo, Record record, String pKey, Page page) {
        page.add(recordNo, record);
        clusteringKeyMin.add(page.getPageNumber(), (Comparable) page.getRecords().getFirst().hashtable().get(pKey));
        saveTable();
    }

    public Record removeRecord(int recordNo, String pKey, Page page) {
        Record htbl = page.remove(recordNo);
        if (page.isEmpty()) {
            String pageName = Paths.get((String) DBApp.getDbConfig().get("DataPath"),
                    tableName, page.getPageNumber() + ".ser").toAbsolutePath().toString();
            int index = pagesPath.indexOf(pageName);
            pagesPath.remove(index);
            clusteringKeyMin.remove(index);
        } else {
            clusteringKeyMin.add(page.getPageNumber(), (Comparable) page.getRecords().getFirst().hashtable().get(pKey));
        }
        saveTable();

        return htbl;
    }

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
     * Saves the table to a file
     *
     * @param tableName the table to save
     * @return table deserialize the table from the file
     */

    public static Table loadTable(String tableName) throws DBAppException {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, tableName + ".ser");

        if (!path.toFile().exists()) {
            throw new DBAppException("Table doesn't exit");
        }

        Table table;
        try (
                FileInputStream fileIn = new FileInputStream(path.toAbsolutePath().toString());
                ObjectInputStream in = new ObjectInputStream(fileIn)) {
            table = (Table) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    public @NotNull Iterator<Page> iterator() {
        return new TableIterator();
    }

    @Override
    public Table clone() {
        try {
            Table clone = (Table) super.clone();
            clone.pagesPath = (Vector<String>) pagesPath.clone();
            clone.clusteringKeyMin = (Vector<Comparable>) clusteringKeyMin.clone();

            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    private class TableIterator implements Iterator<Page> {
        private int pageIndex;
        private Page page;

        public TableIterator() {
            pageIndex = 0;
            page = null;
        }

        @Override
        public boolean hasNext() {
            return pageIndex < pagesCount();
        }

        @Override
        public Page next() { //this method is used to get the next page
            if (!hasNext()) {
                throw new RuntimeException("No more records");
            }

            page = getPage(pageIndex);
            pageIndex++;

            return page;
        }
    }
}

