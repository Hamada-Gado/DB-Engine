package DB;

import java.io.*;
import java.util.Vector;

public class Table implements Serializable {
    private Vector<String> pages;
    public Table() {
        pages = new Vector<String>();
    }

    public void addPage(Page page) {
        if (page == null) {
            throw new RuntimeException("Page is null");
        }

        String path = "data/" + page.hashCode() + ".ser";
        try {
            FileOutputStream fileOut = new FileOutputStream(path);
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
        String path = pages.get(index);
        if (path == null) {
            System.out.println("Page not found");
            throw new DBAppException("Page not found");
        }

        Page page;
        try {
            FileInputStream fileIn = new FileInputStream(path);
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
       int index = pages.indexOf(pageName);
         if (index == -1) {
             throw new DBAppException("Page not found");
         } else {
              return getPage(index);
         }
    }
}
