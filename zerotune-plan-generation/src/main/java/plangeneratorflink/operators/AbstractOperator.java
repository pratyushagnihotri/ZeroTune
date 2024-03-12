package plangeneratorflink.operators;

import plangeneratorflink.operators.source.AdvertisementSourceOperator;
import plangeneratorflink.operators.source.FileSpikeDetectionSourceOperator;
import plangeneratorflink.operators.source.SmartGridSourceOperator;
import plangeneratorflink.operators.source.SyntheticSourceOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.RanGen;

import java.util.HashMap;

public abstract class AbstractOperator<T> {
    protected String type; // WindowOperator, MapOperator ...
    protected Class<?>
            klass; // Class that this operator applies to, like filter for Strings (not valid for
                   // all types)
    protected T function; // The operator function itself
    protected String id; // Unique identifier

    public AbstractOperator() {
        this.type = this.getClass().getSimpleName();
    }

    /**
     * This is called for each operator when building the final graph object. Note that this is
     * often overwritten in the single operators.
     *
     * @return A hash map that contains descriptions (=features) for the operator
     */
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = new HashMap<>();
        description.put(Constants.Features.id.name(), id);
        description.put(Constants.Features.operatorType.name(), type);
        // if (klass != null) {
        //    description.put(Constants.Features.dataType.name(), klass.getSimpleName());
        // }
        return description;
    }

    public T getFunction() {
        return function;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Class<?> getKlass() {
        return klass;
    }
}
