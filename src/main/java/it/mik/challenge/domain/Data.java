package it.mik.challenge.domain;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *  This class represents the input data.
 *  It has a list of {@link it.mik.challenge.domain.Row} and an array of Strings that represent the its relative headers.
 *
 * @author Michele Ruggiero
 *
 */

public class Data{

    private final String[] headers;
    private final List<Row> rows;

    public Data(String[] headers, List<Row> rows) {
        this.headers = headers;
        this.rows = rows;
    }

    public List<String> getHeadersList(){
        return Collections.unmodifiableList(Arrays.asList(headers));
    }

    public List<Row> getRows(){
        return Collections.unmodifiableList(rows);
    }

    @Override
    public String toString() {
        return "Data{" +
                "headers=" + Arrays.toString(headers) +
                ", rows=" + rows +
                '}';
    }



}
