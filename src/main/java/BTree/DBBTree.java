package BTree;

import DB.DBApp;
import DB.DBAppException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * The HashMap is of the form (page number, count of the value in the page)
 * Example:
 * John
 * /   \
 * John Zed ------------
 * |                   |
 * {0: 5, 1: 3, 4: 1} {0: 2, 1: 1, 4: 1}
 */
public class DBBTree<TKey extends Comparable<TKey>> extends BTree<TKey, HashMap<Integer, Integer>> implements Serializable {

    private final String tableName;
    private final String indexName;

    public DBBTree(String tableName, String indexName) {
        super();
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public HashMap<Integer, Integer> search(TKey key) {
        return super.search(key);
    }

    public void insert(TKey key, Integer value) {
        HashMap<Integer, Integer> values = this.search(key);
        if (values == null) {
            values = new HashMap<>();
        }
        int count = values.getOrDefault(value, 0);
        values.put(value, count + 1);
        super.insert(key, values);

        this.saveIndex();
    }


// This is prob useless
//    public void insert(TKey key, Integer value, int prevValue) {
//        HashMap<Integer, Integer> values = this.search(key);
//
//        if (values == null) {
//            this.insert(key, value);
//            return;
//        }
//
//        int count = values.getOrDefault(prevValue, 0);
//        if (count == 1) {
//            values.remove(prevValue);
//        } else {
//            values.put(prevValue, count - 1);
//        }
//
//        this.insert(key, value);
//    }

    public void delete(TKey key, Integer value) {
        HashMap<Integer, Integer> values = this.search(key);
        if (values == null) {
            return;
        }
        int count = values.getOrDefault(value, 0);
        if (count == 1) {
            values.remove(value);
            if (values.isEmpty()) {
                super.delete(key);
            } else {
                super.insert(key, values);
            }
        } else {
            values.put(value, count - 1);
            super.insert(key, values);
        }

        this.saveIndex();
    }

    // save the bplustree index to the disk
    public void saveIndex() {
        Path file = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, indexName + ".ser");

        try (
                FileOutputStream fileOut = new FileOutputStream(file.toAbsolutePath().toString());
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    // fetch the bplustree index from the disk
    public static DBBTree loadIndex(String tableName, String indexName) throws DBAppException {
        DBBTree tree;
        Path file = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, indexName + ".ser");

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
