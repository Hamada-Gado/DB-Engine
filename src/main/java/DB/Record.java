package DB;

import java.io.Serializable;
import java.util.Hashtable;

public record Record(Hashtable<String, Object> hashtable) implements Serializable {

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();

        for (String key : hashtable.keySet()) {
            string.append(hashtable.get(key)).append(",");
        }

        if (!string.isEmpty())
            string.deleteCharAt(string.length() - 1);


        return string.toString();
    }
}
