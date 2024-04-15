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

    private final String tableName;
    private final String indexName;

    public DBBTree(String tableName, String indexName) {
        super();
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public LinkedList<Integer> search(TKey key) {
        return super.search(key);
    }

    public void insert(TKey key, Integer value) {
        LinkedList<Integer> values = this.search(key);
        if (values == null) {
            values = new LinkedList<>();
        }
        values.add(value);
        super.insert(key, values);

        this.saveIndex();
    }

    public void delete(TKey key, Integer value) {
        LinkedList<Integer> values = this.search(key);
        if (values != null) {
            values.remove(value);
            if (values.isEmpty()) {
                super.delete(key);
            } else {
                super.insert(key, values);
            }
        }

        this.saveIndex();
    }

    // save the bplustree index to the disk
    public void saveIndex() {
        Path file = Paths.get((String) DBApp.getDbConfig().get("DataPath"), tableName, indexName + ".ser");

        try (
                FileInputStream fileIn = new FileInputStream(file.toAbsolutePath().toString());
                ObjectInputStream in = new ObjectInputStream(fileIn)) {
            in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // fetch the bplustree index from the disk
    public static DBBTree loadIndex(String TableName, String IndexName) throws DBAppException {
        DBBTree tree;
        Path file = Paths.get((String) DBApp.getDbConfig().get("DataPath"), TableName, IndexName + ".ser");

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
