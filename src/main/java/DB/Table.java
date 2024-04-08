package DB;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Vector;

/**
 * @author ahmedgado
 */

public class Table implements Iterable<Page>, Serializable {
    private final String tableName;
    private final Vector<Path> pages;

    public Table(String tableName) {
        this.tableName = tableName;
        pages = new Vector<>();
    }

    /**
     * Saves the table to a file
     *
     * @param tableName the table to save
     * @return table deserialize the table from the file
     */
    public static Table loadTable(String tableName) {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName + ".ser");
        Table table;
        try {
            FileInputStream fileIn = new FileInputStream(path.toAbsolutePath().toString());
            ObjectInputStream in = new ObjectInputStream(fileIn);
            table = (Table) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    /**
     * @param page the page to add
     *             serialize the page and add its path to the table
     */
    public void addPage(@NotNull Page page) {
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), page.hashCode() + ".ser");
        try {
            FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        pages.add(path);
    }

    /**
     * @param index the index of the page
     * @return the name of the table
     */
    public Page getPage(int index) {
        Path path = pages.get(index);

        Page page;
        try {
            FileInputStream fileIn = new FileInputStream(path.toAbsolutePath().toString());
            ObjectInputStream in = new ObjectInputStream(fileIn);
            page = (Page) in.readObject();
            in.close();
            fileIn.close();
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
        Path path = Paths.get((String) DBApp.getDbConfig().get("DataPath"), pageName + ".ser");
        int index = pages.indexOf(path);
        if (index == -1) {
            throw new RuntimeException("Page not found");
        } else {
            return getPage(index);
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
            if (!pages.isEmpty()) {
                page = getPage(pageIndex);
            }
        }

        @Override
        public boolean hasNext() {
            return page != null && pageIndex < pages.size();
        }

        @Override
        public Page next() {
            if (!hasNext()) {
                throw new RuntimeException("No more records");
            }

            pageIndex++;
            page = null;
            if (pageIndex < pages.size()) {
                page = getPage(pageIndex);
            }

            return page;
        }
    }
}
