package DB;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author ahmedgado
 */

public class Table implements Iterable<Page>, Serializable {
    private final String tableName;
    private final Vector<String> pagesPath;
    private final Vector<Comparable> clusteringKeyMin;

    public Table(String tableName) {
        this.tableName = tableName;
        pagesPath = new Vector<>();
        clusteringKeyMin = new Vector<>();
    }

    public String getTableName() {
        return tableName;
    }

    public Vector<String> getPagesPath() {
        return pagesPath;
    }

    public Vector<Comparable> getClusteringKeyMin() {
        return clusteringKeyMin;
    }

    /**
     * Serialize the table only not the pages
     */
    public void updateTable() {
        Path path = Paths.get((String) DBApp.getDb_config().get("DataPath"), tableName, tableName + ".ser");
        try (
                FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param page the page to add
     *             <p>
     *             serialize the page and add its path to the table
     */

    public void addPage(@NotNull Page page) {
        Path path = Paths.get((String) DBApp.getDb_config().get("DataPath"), tableName, page.getPageNumber() + ".ser");
        page.updatePage();
        pagesPath.add(path.toAbsolutePath().toString());
        updateTable();
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
        Path path = Paths.get((String) DBApp.getDb_config().get("DataPath"), tableName, pageName + ".ser");
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

    public void addRecord(Hashtable<String, Object> record, String pKey, Page page) {
        page.add(record);
        clusteringKeyMin.add(page.getPageNumber(), (Comparable) page.getRecords().get(0).get(pKey));
        updateTable();
    }

    public void addRecord(int recordNo, Hashtable<String, Object> record, String pKey, Page page) {
        page.add(recordNo, record);
        clusteringKeyMin.add(page.getPageNumber(), (Comparable) page.getRecords().get(0).get(pKey));
        updateTable();
    }

    public Hashtable<String, Object> removeRecord(int recordNo, Page page) {
        Hashtable htbl = page.remove(recordNo);
        clusteringKeyMin.add(page.getPageNumber(), (Comparable) page.getRecords().get(0));
        updateTable();

        return htbl;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        for (Page page : this) {
            res.append("Page:\n")
                    .append(page)
                    .append("\n");
        }
        res.deleteCharAt(res.length() - 1);

        return res.toString();
    }

    /**
     * Saves the table to a file
     *
     * @param tableName the table to save
     * @return table deserialize the table from the file
     */
    public static Table loadTable(String tableName) throws DBAppException {
        Path path = Paths.get((String) DBApp.getDb_config().get("DataPath"), tableName, tableName + ".ser");
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

    public static void deleteTable(String tableName) {
        Path path = Paths.get((String) DBApp.getDb_config().get("DataPath"), tableName);
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public @NotNull Iterator<Page> iterator() {
        return new TableIterator();
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

    public List<Hashtable<String, Object>> getRecords() {
        List<Hashtable<String, Object>> allRecords = new ArrayList<>();
        Iterator<Page> pageIterator = this.iterator();
        while (pageIterator.hasNext()) {
            Page page = pageIterator.next();
            allRecords.addAll(page.getRecords());
        }
        return allRecords;
    }
}
