package DB;

import java.io.Serializable;
import java.util.Hashtable;

/**
 * This class represents a Record object that implements Serializable interface.
 * It uses a Hashtable to store the record data.
 */
public record Record(Hashtable<String, Object> hashtable) implements Serializable {

    /**
     * This method overrides the toString method from the Object class.
     * It iterates over the Hashtable and appends each value to a StringBuilder.
     * If the StringBuilder is not empty, it removes the last comma.
     *
     * @return A string representation of the Hashtable values.
     */
    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();

        // Iterate over the Hashtable keys
        for (String key : hashtable.keySet()) {
            // Append each value to the StringBuilder
            string.append(hashtable.get(key)).append(",");
        }

        // If the StringBuilder is not empty, remove the last comma
        if (!string.isEmpty())
            string.deleteCharAt(string.length() - 1);

        // Return the string representation of the Hashtable values
        return string.toString();
    }
}