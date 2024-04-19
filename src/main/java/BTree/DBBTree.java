package BTree;

import DB.DBApp;
import DB.DBAppException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * This class represents a B+ tree index for a database table.
 * The keys in the B+ tree are the values of the indexed column.
 * The values in the B+ tree are HashMaps that map page numbers to the count of the key in the page.
 * The B+ tree supports search, range search, insert, and delete operations.
 * The B+ tree can be saved to disk and loaded from disk.
 */
public class DBBTree<TKey extends Comparable<TKey>> extends BTree<TKey, HashMap<Integer, Integer>> implements Serializable {

    private final String tableName;
    private final String indexName;

    /**
     * Constructs a new B+ tree index for a given table.
     *
     * @param tableName The name of the table.
     * @param indexName The name of the index.
     */
    public DBBTree(String tableName, String indexName) {
        super();
        this.tableName = tableName;
        this.indexName = indexName;
    }

    /**
     * Searches for a key in the B+ tree.
     *
     * @param key The key to search for.
     * @return A HashMap that maps page numbers to the count of the key in the page.
     */
    @Override
    public HashMap<Integer, Integer> search(TKey key) {
        return super.search(key);
    }

    /**
     * Searches for a range of keys in the B+ tree.
     *
     * @param lowerBound The lower bound of the range.
     * @param upperBound The upper bound of the range.
     * @return A HashSet of page numbers that contain keys in the range.
     */
    public HashSet<Integer> searchRange(TKey lowerBound, TKey upperBound) {
        LinkedList<HashMap<Integer, Integer>> res = super.search(lowerBound, upperBound);
        HashSet<Integer> set = new HashSet<>();
        for (HashMap<Integer, Integer> map : res) {
            set.addAll(map.keySet());
        }

        return set;
    }

    /**
     * Inserts a key-value pair into the B+ tree.
     * If the key does not exist in the B+ tree, a new entry is created.
     * If the key exists in the B+ tree, the count of the key in the page is incremented.
     *
     * @param key   The key to insert.
     * @param value The value to insert.
     */
    public void insert(TKey key, Integer value) {
        HashMap<Integer, Integer> values = this.search(key);
        if (values == null) {
            values = new HashMap<>();
            values.put(value, 1);
            super.insert(key, values);
        } else {
            int count = values.getOrDefault(value, 0);
            values.put(value, count + 1);
        }

        this.saveIndex();
    }

    /**
     * Deletes a key-value pair from the B+ tree.
     * If the count of the key in the page is 1, the key is removed from the B+ tree.
     * If the count of the key in the page is greater than 1, the count is decremented.
     *
     * @param key   The key to delete.
     * @param value The value to delete.
     */
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
            }
        } else {
            values.put(value, count - 1);
        }

        this.saveIndex();
    }

    /**
     * Saves the B+ tree index to disk.
     */
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

    /**
     * Loads the B+ tree index from disk.
     *
     * @param tableName The name of the table.
     * @param indexName The name of the index.
     * @return The loaded B+ tree index.
     * @throws DBAppException If the index does not exist.
     */
    public static DBBTree loadIndex(String tableName, String indexName) throws DBAppException {
        DBBTree tree;
        Path file = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, indexName + ".ser");

        if (!file.toFile().exists()) {
            throw new DBAppException("Index " + indexName + " does not exist");
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