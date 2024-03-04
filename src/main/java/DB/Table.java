package DB;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Vector;

public class Table implements Serializable {
    private String tableName;
    private Vector<Path> pages;

    public Table(String tableName) {
        pages = new Vector<>();
        this.tableName = tableName;
    }

    public void addPage(Page page) {
        if (page == null) {
            throw new RuntimeException("Page is null");
        }

        Path path = Paths.get("resources", "data", page.hashCode() + ".ser");
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

    public Page getPage(int index) throws DBAppException {
        Path path = pages.get(index);
        if (path == null) {
            throw new DBAppException("Page not found");
        }

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

    public Page getPage(String pageName) throws DBAppException {
        Path path = Paths.get("resources", "data", pageName + ".ser");
        int index = pages.indexOf(path);
        if (index == -1) {
            throw new DBAppException("Page not found");
        } else {
            return getPage(index);
        }
    }
}
