package plangeneratorflink.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;

public class RanGen {
    /** @return Random boolean */
    private static final Random ran = new Random();

    public static boolean randomBoolean() {
        return ran.nextBoolean();
    }

    public static int randIntFromList(int[] intList) {
        return intList[ran.nextInt(intList.length)];
    }

    public static int randIntRange(Tuple2<Integer, Integer> pair) {
        return randInt(pair.f0, pair.f1);
    }

    /** Random integer within an inclusive lower and inclusive upper bound. */
    public static int randInt(int min, int max) {
        if (min >= max) {
            return min;
        }
        return ran.nextInt(max + 1 - min) + min;
    }

    public static double randDoubleRange(Tuple2<Double, Double> pair) {
        return randDouble(pair.f0, pair.f1);
    }

    public static double randDouble(double min, double max) {
        return round(min + (ran.nextDouble() * (max - min)), 2);
    }

    public static double randDouble() {
        return ran.nextDouble();
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    /**
     * @param len Length of string
     * @return Random string with given length.
     */
    public static String randString(int len) {
        String allChars = Constants.Synthetic.Train.ALL_CHARS;
        if (len == 1) {
            return String.valueOf(allChars.charAt(randInt(0, allChars.length() - 1)));
        }
        StringBuilder a = new StringBuilder();
        for (int i = 0; i < len; i++) {
            a.append(allChars.charAt(ran.nextInt(allChars.length())));
        }
        return a.toString();
    }

    /**
     * @param klass Class to get literal from
     * @return Random literal given the specification in PGConfig
     */
    public static Object generateRandomLiteral(Class<?> klass) {
        Object literal;
        if (klass == Integer.class) {
            literal = randIntRange(Constants.Synthetic.Train.INTEGER_VALUE_RANGE);

        } else if (klass == String.class) {
            literal = randString(Constants.Synthetic.Train.STRING_LENGTH); // use only first letter

        } else if (klass == Double.class) {
            literal = randDoubleRange(Constants.Synthetic.Train.DOUBLE_VALUE_RANGE);

        } else {
            throw new RuntimeException("Class is not supported");
        }
        return literal;
    }

    public static double randDoubleFromList(double[] doubleList) {
        return doubleList[ran.nextInt(doubleList.length)];
    }

    public static Class<?> randClass() {
        switch (randInt(0, 3)) {
            case 0:
                return Integer.class;
            case 1:
                return String.class;
            default:
                return Double.class;
        }
    }
}
