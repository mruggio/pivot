package it.mik.challenge;

import it.mik.challenge.builder.AggregationBuilder;
import it.mik.challenge.builder.DataBuilder;
import it.mik.challenge.domain.Aggregation;
import it.mik.challenge.domain.Data;
import it.mik.challenge.domain.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collector;

import static java.util.stream.Collectors.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 * @author Michele Ruggiero
 *
 */

public class TestPivot {

    private static Data data;

    @BeforeAll
    static void setup() {

        data = new DataBuilder()
                .addHeaders("Nation", "Eyes", "Hair")
                .addRow(103, "Germany", "Dark", "Brown")
                .addRow(168, "Germany", "Green", "Brown")
                .addRow(907, "Spain", "Dark", "Black")
                .addRow(468, "Germany", "Dark", "Black")
                .addRow(389, "Germany", "Blue", "Brown")
                .addRow(753, "Germany", "Brown", "Red")
                .addRow(536, "Germany", "Green", "Red")
                .addRow(359, "Spain", "Green", "Brown")
                .addRow(498, "France", "Blue", "Black")
                .addRow(906, "Germany", "Green", "Red")
                .addRow(506, "France", "Blue", "Black")
                .addRow(778, "Spain", "Brown", "Red")
                .addRow(857, "France", "Green", "Black")
                .addRow(148, "Italy", "Dark", "Black")
                .addRow(288, "France", "Green", "Blonde")
                .addRow(852, "Spain", "Blue", "Black")
                .getData();
    }

    @Test
    void testSumForEachLevel() {

        Aggregation aggr = new AggregationBuilder()
                .generateAggregation(
                        data,
                        summingInt(r -> r.getValue().intValue()),
                        Arrays.asList("Nation", "Eyes", "Hair")
                );

        //Grand Total
        assertThat(aggr.getChild("grand_total").getValue(), is(8516));

        //Nation
        assertThat(aggr.getChild("grand_total").getChild("France").getValue(), is(2149));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getValue(), is(3323));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getValue(), is(148));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getValue(), is(2896));

        //Nation -> Eyes
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getValue(), is(1004));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getValue(), is(1145));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getValue(), is(389));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getValue(), is(753));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getValue(), is(571));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getValue(), is(1610));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getValue(), is(148));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getValue(), is(852));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getValue(), is(778));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getValue(), is(907));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getValue(), is(359));

        //Nation -> Eyes -> Hair
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Blonde").getValue(), is(288));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Black").getValue(), is(857));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Black").getValue(), is(1004));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Brown").getValue(), is(168));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Red").getValue(), is(1442));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Brown").getValue(), is(389));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Red").getValue(), is(753));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Black").getValue(), is(468));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Brown").getValue(), is(103));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Black").getValue(), is(148));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Brown").getValue(), is(359));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Black").getValue(), is(852));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Red").getValue(), is(778));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Black").getValue(), is(907));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
    }

    @Test
    void testAvgForEachLevel() {

        Aggregation aggr = new AggregationBuilder()
                .generateAggregation(
                        data,
                        averagingInt(r -> r.getValue().intValue()),
                        Arrays.asList("Nation", "Eyes", "Hair")
                );

        //Grand Total
        assertThat(aggr.getChild("grand_total").getValue(), is(532.25D));

        //Nation
        assertThat(aggr.getChild("grand_total").getChild("France").getValue(), is(537.25D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getValue(), is(474.7142857142857D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getValue(), is(148.0D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getValue(), is(724D));

        //Nation -> Eyes
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getValue(), is(502D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getValue(), is(572.5D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getValue(), is(389D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getValue(), is(753D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getValue(), is(285.5D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getValue(), is(536.6666666666666D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getValue(), is(148D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getValue(), is(852D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getValue(), is(778D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getValue(), is(907D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getValue(), is(359D));

        //Nation -> Eyes -> Hair
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Blonde").getValue(), is(288D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Black").getValue(), is(857D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Black").getValue(), is(502D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Brown").getValue(), is(389D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Black").getValue(), is(468D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Brown").getValue(), is(103D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Brown").getValue(), is(168D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Red").getValue(), is(721D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Red").getValue(), is(753D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Black").getValue(), is(148D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Black").getValue(), is(852D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Black").getValue(), is(907D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Brown").getValue(), is(359D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Red").getValue(), is(778D));
    }

    @Test
    void testChangeOrderWithSum() {
        Aggregation aggr = new AggregationBuilder()
                .generateAggregation(
                        data,
                        summingInt(r -> r.getValue().intValue()),
                        Arrays.asList("Eyes", "Nation", "Hair")
                );

        //Grand Total
        assertThat(aggr.getChild("grand_total").getValue(), is(8516));

        //Eyes
        assertThat(aggr.getChild("grand_total").getChild("Dark").getValue(), is(1626));
        assertThat(aggr.getChild("grand_total").getChild("Green").getValue(), is(3114));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getValue(), is(1531));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getValue(), is(2245));

        //Eyes -> Nation
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Germany").getValue(), is(571));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Italy").getValue(), is(148));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Spain").getValue(), is(907));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("France").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Germany").getValue(), is(1610));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Italy").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Spain").getValue(), is(359));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("France").getValue(), is(1145));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Germany").getValue(), is(753));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Italy").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Spain").getValue(), is(778));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("France").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Germany").getValue(), is(389));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Italy").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Spain").getValue(), is(852));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("France").getValue(), is(1004));

        //Eyes -> Nation -> Hair
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Germany").getChild("Brown").getValue(), is(103));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Germany").getChild("Black").getValue(), is(468));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Germany").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Germany").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Italy").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Italy").getChild("Black").getValue(), is(148));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Italy").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Italy").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Spain").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Spain").getChild("Black").getValue(), is(907));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Spain").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("Spain").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("France").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("France").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("France").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Dark").getChild("France").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Germany").getChild("Brown").getValue(), is(168));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Germany").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Germany").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Germany").getChild("Red").getValue(), is(1442));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Italy").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Italy").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Italy").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Italy").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Spain").getChild("Brown").getValue(), is(359));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Spain").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Spain").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("Spain").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("France").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("France").getChild("Black").getValue(), is(857));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("France").getChild("Blonde").getValue(), is(288));
        assertThat(aggr.getChild("grand_total").getChild("Green").getChild("France").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Germany").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Germany").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Germany").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Germany").getChild("Red").getValue(), is(753));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Italy").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Italy").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Italy").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Italy").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Spain").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Spain").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Spain").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("Spain").getChild("Red").getValue(), is(778));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("France").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("France").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("France").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Brown").getChild("France").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Germany").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Germany").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Germany").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Germany").getChild("Brown").getValue(), is(389));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Italy").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Italy").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Italy").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Italy").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Spain").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Spain").getChild("Black").getValue(), is(852));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Spain").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("Spain").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("France").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("France").getChild("Black").getValue(), is(1004));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("France").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Blue").getChild("France").getChild("Red").getValue(), is(nullValue()));
    }

    @Test
    void testCustomFunctionForEachLevel() {

        Collector<Row, ArrayList<Double>, Double> customFunction = Collector.of(
                () -> new ArrayList(),
                (result, e) -> result.add(e.getValue().doubleValue()),
                (result1, result2) -> {
                    result1.addAll(result2);
                    return result1;
                },
                r -> {
                    if (r.size() % 2 == 0) {
                        return r.stream()
                                .sorted()
                                .collect(toList())
                                .subList((r.size() - 1) / 2, (r.size() + 2) / 2)
                                .stream()
                                .collect(collectingAndThen(toList(), e -> e.stream().mapToDouble(Double.class::cast)
                                        .sum() / 2))
                                ;
                    } else {
                        return r.stream()
                                .sorted()
                                .collect(toList())
                                .get(r.size() / 2);
                    }
                }
        );

        Aggregation aggr = new AggregationBuilder()
                .generateAggregation(
                        data,
                        customFunction,
                        Arrays.asList("Nation", "Eyes", "Hair")
                );

        //Grand Total
        assertThat(aggr.getChild("grand_total").getValue(), is(502D));

        //Nation
        assertThat(aggr.getChild("grand_total").getChild("Germany").getValue(), is(468D));
        assertThat(aggr.getChild("grand_total").getChild("France").getValue(), is(502D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getValue(), is(148.0D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getValue(), is(815D));

        //Nation -> Eyes
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getValue(), is(502D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getValue(), is(572.5D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getValue(), is(389D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getValue(), is(753D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getValue(), is(285.5D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getValue(), is(536D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getValue(), is(148D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getValue(), is(852D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getValue(), is(778D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getValue(), is(907D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getValue(), is(359D));

        //Nation -> Eyes -> Hair
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Blonde").getValue(), is(288D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Black").getValue(), is(857D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Green").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Black").getValue(), is(502D));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("France").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Red").getValue(), is(721D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Green").getChild("Brown").getValue(), is(168D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Blue").getChild("Brown").getValue(), is(389D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Black").getValue(), is(468D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Dark").getChild("Brown").getValue(), is(103D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Red").getValue(), is(753D));
        assertThat(aggr.getChild("grand_total").getChild("Germany").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Green").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Black").getValue(), is(148D));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Italy").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Green").getChild("Brown").getValue(), is(359D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Black").getValue(), is(852D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Blue").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Black").getValue(), is(907D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Red").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Dark").getChild("Brown").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Blonde").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Black").getValue(), is(nullValue()));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Red").getValue(), is(778D));
        assertThat(aggr.getChild("grand_total").getChild("Spain").getChild("Brown").getChild("Brown").getValue(), is(nullValue()));
    }

}
