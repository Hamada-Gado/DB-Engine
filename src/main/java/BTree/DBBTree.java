package BTree;

import DB.DBApp;
import DB.DBAppException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;

public class DBBTree<TKey extends Comparable<TKey>> extends BTree<TKey, LinkedList<Integer>> implements Serializable {

    public LinkedList<Integer> search(TKey key) {
        return this.search(key);
    }

    // fetch the bplustree index from the disk
    public static DBBTree loadIndex(String TableName, String IndexName) throws DBAppException {
        DBBTree tree;
        Path file = Paths.get(DBApp.getDbConfig().get("DataPath") + "/" + TableName + "/" + IndexName + ".ser");

        if (!file.toFile().exists()) {
            throw new DBAppException("Index file does not exist");
        }

        try (
                FileInputStream fileIn = new FileInputStream(file.toAbsolutePath().toString());
                ObjectInputStream in = new ObjectInputStream(fileIn)) {
            tree = (DBBTree) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return tree;
    }

}
