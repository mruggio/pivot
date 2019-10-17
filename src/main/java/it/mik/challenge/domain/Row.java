package it.mik.challenge.domain;


import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the labels' row.
 * The labels are organized in a map, that aggregates them by their headers.
 * It contains the relative value for each row.
 *
 * @author Michele Ruggiero
 *
 */

public class Row {

    private Map<String, String> labelsByHeader;
    private Number value;

    public Row() {
        this.labelsByHeader = new HashMap<>();
        this.labelsByHeader.put("all", "grand_total"); //necessary to calculate the grand total
    }

    public Row addLabel(String header, String label){
        labelsByHeader.put(header, label);
        return this;
    }

    /**
     * <p>
     *     Calculates the key that will be used for grouping that will determine the groups' members.
     * </p>
     * @param headers
     * @return String
     *
     */
    public String getKeys(String headers) {
        StringBuilder stringBuilder = new StringBuilder();
        if(headers != null) {
            String prefix = "";
            for (String s : headers.split(" ")) {
                stringBuilder.append(prefix);
                prefix = " ";
                stringBuilder.append(labelsByHeader.get(s));
            }
        }

        return stringBuilder.toString();
    }

    public Number getValue() {
        return value;
    }

    public Row setValue(Number value) {
        this.value = value;
        return this;
    }

    @Override
    public String toString() {
        return "Row{" +
                "labels=" + labelsByHeader.values() +
                ", value=" + value +
                '}';
    }
}
