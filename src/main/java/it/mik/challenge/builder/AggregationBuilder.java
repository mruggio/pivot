package it.mik.challenge.builder;

import it.mik.challenge.domain.Aggregation;
import it.mik.challenge.domain.Data;
import it.mik.challenge.domain.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;


/**
 *  This builder helps you generating the aggregations.
 *
 * @author Michele Ruggiero
 *
 */

public class AggregationBuilder {

    /**
     * <p>
     *      It generates the tree aggregation which is based on data, aggregation order and the specified function.
     * </p>
     * @param data contains the row and headers which describe the domain of belonging for each relative label.
     * @param function will be applicated on the value for each row, grouped by keys.
     * @param aggrOrder specifies the order aggregation of the headers.
     * @return tree of Aggregation
     *
     */
    public Aggregation generateAggregation(Data data, Collector<Row, ?, ? extends Number> function, List<String> aggrOrder){

        Aggregation ag = new Aggregation();
        List<String> perm = Stream.concat(Stream.of("all"), aggrOrder.stream()).collect(toKeyPermutations);
        perm.forEach(key ->
            ag.addAggr(data.getRows()
                    .stream()
                    .collect(
                            groupingBy(r -> r.getKeys(key), collectingAndThen(function, r->(Number)r))
                    ),
                    aggrOrder
            )
        );

        return ag;
    }


    /**
     * This Collector generates the keys permutation, that represents the groups of labels necessary to
     * calculate aggregation and sub-aggregation.
     * Example:
     *      input=Germany Brown Red
     *      output= [   Germany,            //key1
     *                  Germany Brown,      //key2
     *                  Germany Brown Red   //key3
     *               ]
     */
    private Collector<String, ArrayList<String>, List<String>> toKeyPermutations = Collector.of(
            () -> new ArrayList<>(),
            (res, item) -> {
                if (res.size() > 0) {
                    String s = res.get(res.size() - 1);
                    res.add(s.concat(" "+item));
                } else {
                    res.add(item);
                }
            },
            (result1, result2) -> {
                result1.addAll(result2);
                return result1;
            },
            c -> c.stream()
                    .map(p -> p.split(","))
                    .map(r -> Arrays.asList(r))
                    .map(a -> a.stream().collect(joining(" ")))
                    .collect(Collectors.toList())
    );

}
