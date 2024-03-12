class CLI:
    TEST = "test"
    TRAIN = "train"


class Metrics:
    LAT = "latency"
    TPT = "throughput"


class Feat:
    # FLINK FEATURE DEFINITION
    # Source related
    EVENT_RATE = "confEventRate"
    NUM_DOUBLE = "numDouble"
    NUM_INTEGER = "numInteger"
    NUM_STRING = "numString"

    # Data characteristic related
    TUPLE_W_IN = "tupleWidthIn"
    TUPLE_W_OUT = "tupleWidthOut"
    SELECTIVITY = "avgRealSelectivity"
    # SELECTIVITY = "realSelectivity"

    # Placement related
    INSTANCES = "instances"
    PARALLELISM = "parallelism"
    COMPONENT = "component"
    MAX_CPU_FREQ = "maxCPUFreq"
    NETWORK_LINK_SPEED = "networkLinkSpeed"
    PHYSICAL_CPU_CORES = "physicalCPUCores"
    TOTAL_MEMORY = "totalMemory"

    # Join related
    JOIN_CLASS = "joinKeyClass"

    # Window related
    WINDOW_POLICY = "windowPolicy"
    WINDOW_TYPE = "windowType"
    SLIDING_LENGTH = "slidingLength"
    WINDOW_LENGTH = "windowLength"

    # Aggregation related
    AGG_FUNCTION = "aggFunction"
    AGG_CLASS = "aggClass"
    KEY_BY_CLASS = "keyByClass"

    # Filter related
    FILTER_FUNCTION = "filterFunction"
    FILTER_CLASS = "filterClass"


class Info:
    # these are not considered as features, and thus we do also not create feature statistics
    INPUT_RATE = "inputRate"
    OUTPUT_RATE = "outputRate"
    OP_TYPE = "operatorType"
    HOST = "host"
    ID = "id"
    LITERAL = "literal"
    CPU_LOAD = "avgCPULoad"


class Featurization():
    no_joins = None
    def __init__(self, no_joins):
        if no_joins:
            # Template 1:
            self.WINDOWED_JOIN_FEATURES = []
        else:
            # Template 2, Template 3, All Templates, ...:
            self.WINDOWED_JOIN_FEATURES = [Feat.JOIN_CLASS, Feat.WINDOW_TYPE, Feat.WINDOW_POLICY,
                                    Feat.SLIDING_LENGTH, Feat.WINDOW_LENGTH] + self.NON_SOURCE_OPERATOR_FEATURES
    # FLINK FEATURE DEFINITION
    PHYSICAL_NODE_FEATURES = [Feat.NETWORK_LINK_SPEED,
                              Feat.TOTAL_MEMORY, Feat.PHYSICAL_CPU_CORES, Feat.MAX_CPU_FREQ]

    OPERATOR_FEATURES = [Feat.TUPLE_W_OUT, Feat.PARALLELISM, Feat.COMPONENT]

    SOURCE_FEATURES = [Feat.NUM_STRING, Feat.NUM_DOUBLE,
                       Feat.NUM_INTEGER, Feat.EVENT_RATE] + OPERATOR_FEATURES

    NON_SOURCE_OPERATOR_FEATURES = [
        Feat.TUPLE_W_IN, Feat.SELECTIVITY] + OPERATOR_FEATURES

    FILTER_FEATURES = [Feat.FILTER_FUNCTION,
                       Feat.FILTER_CLASS] + NON_SOURCE_OPERATOR_FEATURES

    WINDOWED_JOIN_FEATURES = []

    WINDOWED_AGGREGATION = [Feat.WINDOW_LENGTH, Feat.AGG_CLASS, Feat.WINDOW_TYPE, Feat.WINDOW_POLICY,
                            Feat.SLIDING_LENGTH, Feat.AGG_FUNCTION, Feat.KEY_BY_CLASS] + NON_SOURCE_OPERATOR_FEATURES


class Cost_Optimizer:
    PREDICTED_COSTS = "predicted_costs"
    REAL_COSTS = "real_costs"
