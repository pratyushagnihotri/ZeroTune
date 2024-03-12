package plangeneratorflink.operators.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;
import plangeneratorflink.utils.RanGen;

import java.util.HashMap;
import java.util.function.BiFunction;

public class FilterOperator<T> extends AbstractOperator<T> implements FilterFunction<DataTuple> {
    private Class<?> klass;
    private Object literal;
    private BiFunction<T, T, Boolean> function;
    private String filterType;
    private int fieldNumber = -1;

    public FilterOperator(BiFunction<T, T, Boolean> function, String filterType, Class<?> klass) {
        this.function = function;
        this.filterType = filterType;
        this.klass = klass;
        this.literal = RanGen.generateRandomLiteral(klass);
    }

    public Object getLiteral() {
        return this.literal;
    }

    public void setLiteral(Object literal) {
        this.literal = literal;
    }

    public void setFieldNumber(Integer fieldNumber) {
        this.fieldNumber = fieldNumber;
    }

    public Class<?> getFilterKlass() {
        return this.klass;
    }

    @Override
    public boolean filter(DataTuple dataTuple) throws Exception {
        if (this.fieldNumber == -1
                || this.fieldNumber >= dataTuple.maxAmountOfDataType(this.klass)) {
            this.fieldNumber = RanGen.randInt(0, dataTuple.maxAmountOfDataType(this.klass) - 1);
        }
        return this.function.apply(
                (T) dataTuple.getValue(this.klass, this.fieldNumber), (T) this.literal);
    }

    @Override
    public String toString() {
        return "FilterDataTupleOperator [fieldNumber="
                + fieldNumber
                + ", filterType="
                + filterType
                + ", function="
                + function
                + ", klass="
                + klass
                + ", literal="
                + literal
                + "]";
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put(Constants.Features.literal.name(), literal);
        description.put(Constants.Features.filterFunction.name(), filterType);
        description.put(Constants.Features.filterClass.name(), klass.getSimpleName());
        return description;
    }

    public String getFilterType() {
        return filterType;
    }

    @Override
    public T getFunction() {
        return (T) this;
    }
}
