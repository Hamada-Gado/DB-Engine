package DB;

import java.util.Hashtable;

public class Tuple extends Hashtable<String, Object> {
    @Override
    public synchronized String toString() {
        StringBuilder tuple = new StringBuilder();

        for (String key : this.keySet()) {
            tuple.append(this.get(key)).append(",");
        }

        if (!tuple.isEmpty())
            tuple.deleteCharAt(tuple.length() - 1);


        return tuple.toString();
    }
}
