package DB;

import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("ALL")
public class DBAppTest {
    void naiveInsert(String strTableName,
                     Hashtable<String, Object> htblColNameValue, int pageNo, int recordNo) throws DBAppException {
        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);
        if (metaData == null) {
            throw new DBAppException("Table not found");
        }

        String pKey = metaData.get(strTableName).get("clusteringKey")[0];
        Object pValue = htblColNameValue.get(pKey);

        Table currentTable = Table.loadTable(strTableName);

        for (int i = pageNo; i <= currentTable.pagesCount(); i++) {
            if (i < currentTable.pagesCount()) {
                Page page = currentTable.getPage(i);
                if (!currentTable.getPage(i).isFull()) {
                    // e.g page size is 10,but currently it contains 9 records.
                    // If you want to insert into position 4 (if we assume recordNo is the one before where you want to insert then it's 3)
                    // you have to temporarily remove records 4 to 9 to be able to insert
                    page.add(recordNo, htblColNameValue);
                    System.out.println("just add: " + currentTable.getPage(i).getRecords());
                    break;
                } else {
                    page.add(recordNo, htblColNameValue);
                    htblColNameValue = page.remove(currentTable.getPage(i).getMax());
                    System.out.println("let's shift: " + currentTable.getPage(i).getRecords());
                    recordNo = 0;
                }
            } else {
                Page newPage = new Page(strTableName, currentTable.pagesCount(), Integer.parseInt((String) DBApp.getDb_config().get("MaximumRowsCountinPage")));
                newPage.getRecords().add(htblColNameValue);
                currentTable.addPage(newPage);
                System.out.println("new page: " + currentTable.getPage(i).getRecords());
                break;
            }
        }

        currentTable.updateTable();
    }

    @org.junit.jupiter.api.Test
    void testNaiveInsert() {
        String strTableName = "Test";
        DBApp dbApp = new DBApp();

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        try {
            dbApp.createTable(strTableName, "id", htblColNameType);
        } catch (DBAppException e) {
            e.printStackTrace();
            assertTrue(false);
        }

        System.out.println("Table created successfully");

        try {
            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(2343432));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            naiveInsert(strTableName, htblColNameValue, 0, 0);
            System.out.println("Record inserted successfully");

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(453455));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            naiveInsert(strTableName, htblColNameValue, 0, 1);
            System.out.println("Record inserted successfully");

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(5674567));
            htblColNameValue.put("name", new String("Dalia Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.25));
            naiveInsert(strTableName, htblColNameValue, 0, 1);
            System.out.println("Record inserted successfully");

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(23498));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            naiveInsert(strTableName, htblColNameValue, 0, 1);
            System.out.println("Record inserted successfully");

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(78452));
            htblColNameValue.put("name", new String("Zaky Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.88));
            naiveInsert(strTableName, htblColNameValue, 0, 1);
            System.out.println("Record inserted successfully");

            System.out.println("finally\n" + Table.loadTable(strTableName));
        } catch (DBAppException e) {
            e.printStackTrace();
        } finally {
            // Table.deleteTable(strTableName);
        }
    }
}
