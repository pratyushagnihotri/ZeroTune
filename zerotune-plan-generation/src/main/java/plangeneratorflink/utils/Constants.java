package plangeneratorflink.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.List;

public interface Constants {

    int IDLE_BETWEEN_QUERYS_DURATION = 5000; // 5s

    enum Features {
        operatorType,
        id,
        filterFunction,
        filterClass,
        literal,
        aggFunction,
        aggClass,
        keyByClass,
        windowType,
        windowPolicy,
        windowLength,
        slidingLength,
        joinKeyClass,
        parallelism
    }

    enum EnumerationStrategy {
        RANDOM,
        EXHAUSTIV,
        RULEBASED,
        MINAVGMAX,
        INCREASING,
        PARAMETERBASED,
        SHPREDICTIVECOMPARISON
    }

    enum ExperimentInterceptorType {
        EVENTRATE,
    }

    interface EnumerationStrategyParameter {}

    interface Modes {
        String TRAIN = "train";
        String TEST = "test";
        String RandomSD = "randomspikedetection";
        String FileSD = "filespikedetection";
        String AlternativeSD = "alternativespikedetection";
        String SG = "smartgrid";
        String AD = "advertisement";
        String SH = "searchheuristic";
    }

    interface MongoDB {
        String DATABASENAME = "plangeneratorflink";
        String USERNAME = "pgf";
        String PASSWORD = "pwd";

        interface Collection {
            String LABELS = "query_labels";
            String OBSERVATIONS = "query_observations";
            String PLACEMENT = "query_placement";
            String GRAPHS = "query_graphs";
            String GROUPING = "query_grouping";
        }
    }

    interface SD {
        int[] throughputs =
                new int[] {
                    250, 500, 750, 1_000, 1_500, 2_000, 4_000, 10_000, 20_000, 50_000, 150_000,
                    500_000, 1_000_000, 2_000_000
                };
    }

    interface SG {
        int[] throughputs =
                new int[] {
                    250, 500, 750, 1_000, 1_500, 2_000, 4_000, 10_000, 20_000, 50_000, 150_000,
                    500_000, 1_000_000, 2_000_000
                };
    }

    interface AD {
        int[] throughputs =
                new int[] {
                    250, 500, 750,  1_000, 1_500, 2_000, 4_000, 10_000, 20_000, 50_000, 150_000,
                    500_000, 1_000_000, 2_000_000
                };

        enum SourceType {
            impressions,
            clicks
        }
    }

    interface Synthetic {

        // SOURCE_PARALLELISM can be overwritten via a runtime parameter
        int SOURCE_PARALLELISM = 10;

        interface Train {
            /** Linear query */
            String TEMPLATE1 = "template1";
            /** Two way join */
            String TEMPLATE2 = "template2";
            /** Three way join */
            String TEMPLATE3 = "template3";
            /** all templates */
            List<String> TEMPLATES = Arrays.asList(TEMPLATE1, TEMPLATE2, TEMPLATE3);
            /** Tuples per Second */
            int[] EVENT_RATES =
                    new int[] {
                        100, 200, 400, 500, 700, 1_000, 2_000, 3_000, 5_000, 10_000, 20_000, 50_000,
                        100_000, 250_000, 500_000, 1_000_000
                    };
            /**
             * Specify, the range, how often each of [string, integer, double] can occur in a tuple
             */
            Tuple2<Integer, Integer> INTEGER_RANGE = new Tuple2<Integer, Integer>(1, 5);

            Tuple2<Integer, Integer> STRING_RANGE = new Tuple2<Integer, Integer>(1, 5);
            Tuple2<Integer, Integer> DOUBLE_RANGE = new Tuple2<Integer, Integer>(1, 5);
            /** Fixed value range for integers inside a tuple */
            Tuple2<Integer, Integer> INTEGER_VALUE_RANGE = new Tuple2<Integer, Integer>(-10, 10);
            /** Fixed value range for double inside a tuple */
            Tuple2<Double, Double> DOUBLE_VALUE_RANGE = new Tuple2<Double, Double>(-0.05, 0.05);
            /** Fixed value range for strings inside a tuple */
            int STRING_LENGTH = 1;
            /** Possible values for string */
            String ALL_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
            /** Possible window duration times for duration-based windows in milliseconds */
            int[] WINDOW_DURATION = new int[] {250, 500, 1000, 2000, 3000};
            /**
             * Ratio of window slide against window length for sliding windows (count & duration
             * based)
             */
            double[] WINDOW_SLIDING_RATIO = new double[] {0.3, 0.4, 0.5, 0.6, 0.7};
            /** Possible window length for count-based windows in counts */
            int[] WINDOW_LENGTH = new int[] {5, 10, 25, 50, 75, 100};

            interface DeterministicParameter {
                List<String> TEMPLATES = Arrays.asList(TEMPLATE1, TEMPLATE2, TEMPLATE3);
                int EVENT_RATE = 10_000;
                int TUPLE_WIDTH = 10;
                String FILTER_FUNCTION_NAME =
                        "notEquals"; // 3: startsNotWith (String), 5: notEquals (Integer)
                String FILTER_LITERAL_STRING = "a";
                Double FILTER_LITERAL_DOUBLE = 0.00;
                Integer FILTER_LITERAL_INTEGER = 2;
                Integer FILTER_FIELD_NUMBER = 0;
                int AGGREGATE_FUNCTION_INDEX = 2; // double mean
                int WINDOW_TYPE_INDEX =
                        0; // 0: tumbling time, 1: sliding time, 2: tumbling count, 3: sliding count
                int WINDOW_SIZE = 500;
                double WINDOW_SLIDING_RATIO = 0.4;
                Class<?> FILTER_CLASS = Double.class;
                Class<?> KEY_BY_CLASS = Integer.class;
                Class<?> JOIN_BASED_CLASS = String.class;
            }
        }

        interface Test {
            /** Tuple width extrapolation */
            String TESTA = "testA";
            /** Event rate extrapolation */
            String TESTB = "testB";
            /** Time-based window extrapolation */
            String TESTC = "testC";
            /** Count-based window extrapolation */
            String TESTD = "testD";

            String TESTE = "testE";
            /** all tests */
            List<String> TEMPLATES = Arrays.asList(TESTA, TESTB, TESTC, TESTD, TESTE);

            /** for tuplewidth extrapolation */
            int[] TUPLEWIDTHS = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
            /** for eventrate extrapolation */
            int[] EVENT_RATES =
                    new int[] {
                        10, 25, 50, 75, 100, 150, 200, 300, 400, 450, 500, 600, 700, 850, 1_000,
                        1_500, 2_000, 3_000, 4_000, 5_000, 7_500, 10_000, 15_000, 20_000, 35_000,
                        50_000, 100_000, 175_000, 250_000, 375_000, 500_000, 750_000, 1_000_000,
                        1_500_000, 2_000_000, 3_000_000, 4_000_000
                    };
            /** for window extrapolation (time-based) */
            int[] WINDOW_DURATION =
                    new int[] {
                        50, 100, 150, 200, 250, 325, 500, 750, 1000, 1500, 2000, 2500, 3000, 4000,
                        5000, 6000, 7000, 8000, 9000, 10000
                    };
            /** for window extrapolation (count-based) */
            int[] WINDOW_LENGTH =
                    new int[] {
                        2, 3, 4, 5, 7, 10, 17, 25, 37, 50, 62, 75, 82, 100, 150, 200, 250, 300, 350,
                        400
                    };

            double[] WINDOW_SLIDING_RATIO = Train.WINDOW_SLIDING_RATIO;
        }
    }

    interface Operators {
        String AGGREGATE = "agg";
        String WINDOW = "window";
        String FILTER = "filter";
    }
}
