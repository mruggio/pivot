package it.mik.challenge.builder;

import it.mik.challenge.domain.Data;
import it.mik.challenge.domain.Row;

import java.util.ArrayList;
import java.util.List;

/**
 *  This builder helps you generating the input data.
 *
 * @author Michele Ruggiero
 *
 */

public class DataBuilder {

    private String[] headers;
    private List<Row> rows = new ArrayList<>();


    /**
     * <p>
     *      Adds list of headers for a specific row.
     *      This list must be set only one time for a given row, otherwise an
     *      exception will be catched internally (for now).
     * </p>
     * @param headers list of headers.
     * @return DataBuilder fluent API
     *
     */
    public DataBuilder addHeaders(String... headers){

        if(this.headers == null){
            this.headers = headers;
        }else {
            try {
                throw new Exception("already setted!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return this;
    }

    /**
     * <p>
     *      Adds a list of labels to row and its relative number value.
     * </p>
     * @param value a number describing the attribute of the labels set.
     * @param labels that represent the group that identify the relation on the domains.
     * @return DataBuilder fluent API
     *
     */
    public DataBuilder addRow(Number value, String... labels){

        if(headers.length == labels.length){
            Row row = new Row();
            for(int h=0 ; h < headers.length ; h++){
                row.addLabel(headers[h], labels[h])
                        .setValue(value);
            }
            rows.add(row);
        }else{
            try {
                throw new Exception("wrong row!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return this;
    }

    public Data getData(){
        return new Data(headers, rows);
    }
}
