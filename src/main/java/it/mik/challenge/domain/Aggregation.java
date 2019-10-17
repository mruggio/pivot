package it.mik.challenge.domain;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 *  This class represents the tree of aggregation, resulting from grouping based on order.
 *  Each level has the keys to lookup its relative children as sub aggregation.
 *
 * @author Michele Ruggiero
 *
 */

public class Aggregation {

    private Map<String, Aggregation> childrenByLabels = new HashMap<>();
    private Number value;
    private Integer levelNumber = 0;
    private String levelName = "root";

    /**
     * <p>
     *     Adds the aggregation to tree.
     *     This method works recursively to add each sub-aggregation, if it has a subgroup to insert.
     *     By definition the first level is the aggregation's grand total.
     * </p>
     * @param groups is a map, where the keys are a flat group with its relative value.
     *               Example: groups:{
     *                          "Italy" = 300
     *                          "Italy Blue" = 60
     *                          "Italy Blue Blonde" = 30
     *               }
     *               This will generate three level on the tree, four if we consider the grand_total level.
     *
     * @param aggrOrder that specifies how the aggregation will be made.
     * @return DataBuilder fluent API
     *
     */
    public Aggregation addAggr(Map<String, Number> groups, List<String> aggrOrder){
        if(groups != null)
            groups.forEach((k, v) -> addChild(childrenByLabels, aggrOrder,0, k, v));

        return this;
    }

    /**
     * <p>
     *     This allows to explore the tree, by aggregations and sub-aggregations, specifying the key
     *     for each level.
     * </p>
     * @param key that identifies an aggregation and sub-aggregation.
     *            Example: aggr.getChild("Italy").getChild("Blue").getValue();
     * @return Aggregation will be empty if key is not present, otherwise will return the aggregation of the specified key.
     *
     */
    public Aggregation getChild(String key){
        Aggregation a = childrenByLabels.get(key);
        if(a != null){
            return a;
        }else {
            a = new Aggregation();
            a.value = null;
            return a;

        }
    }

    public Number getValue() {
        return value;
    }

    public Integer getLevelNumber() {
        return levelNumber;
    }

    public String getLevelName() {
        return levelName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Aggregation{");
        sb.append("childrenByLabels=").append(childrenByLabels);
        sb.append(", value=").append(value);
        sb.append(", level=").append(levelNumber);
        sb.append('}');
        return sb.toString();
    }

    private Aggregation addChild(Map<String, Aggregation> sub, List<String> aggrOrder,Integer level, String keys, Number value){
        if(keys != null) {
            List<String> elems = Arrays.asList(keys.split(" "));
            if (level == 0)
                aggrOrder = Stream.concat(Stream.of("grand_total"), aggrOrder.stream()).collect(toList());

            //axiom
            if (elems.size() == 1) {
                Aggregation a = new Aggregation();
                a.value = value;
                a.levelName = aggrOrder.get(0);
                a.levelNumber = level + 1;
                sub.put(elems.get(0), a);
                return a;
            } else { //sub-problem
                String nextKey = elems
                        .subList(1, elems.size())
                        .stream()
                        .collect(joining(" "));
                Aggregation subAggr = sub.get(elems.get(0));
                subAggr.levelName = aggrOrder.get(0);
                subAggr.levelNumber = level + 1;

                return subAggr.addChild(subAggr.childrenByLabels, aggrOrder.subList(1, aggrOrder.size()), subAggr.getLevelNumber(), nextKey, value);
            }
        }

        return new Aggregation();
    }

}
