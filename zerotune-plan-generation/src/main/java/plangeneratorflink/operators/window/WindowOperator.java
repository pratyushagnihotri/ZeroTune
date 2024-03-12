package plangeneratorflink.operators.window;

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;

import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;

import java.util.HashMap;

/**
 * WindowOperator represents a WindowAssigner which can be accessed as well as other attributes that
 * define the window.
 *
 * @param <T>
 */
public class WindowOperator<T extends WindowAssigner<Object, ? extends Window>>
        extends AbstractOperator<T> {

    private final String windowType;
    private final String windowPolicy;
    private final Integer windowLength;
    private final Integer slidingLength;
    private Evictor<Object, ? super Window> evictor;

    public WindowOperator(
            String windowType,
            String windowPolicy,
            T windowFunction,
            Integer windowLength,
            Integer slidingLength) {
        this.windowType = windowType;
        this.windowPolicy = windowPolicy;
        this.function = windowFunction;
        this.windowLength = windowLength;
        this.slidingLength = slidingLength;
    }

    public String getWindowPolicy() {
        return this.windowPolicy;
    }

    public Evictor<Object, ? super Window> getEvictor() {
        return this.evictor;
    }

    public void setEvictor(Evictor<Object, ? super Window> evictor) {
        this.evictor = evictor;
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        // window operator cannot have a parallelism, this is handled by the following operator
        // (e.g. aggregate operator)
        description.remove(Constants.Features.parallelism.name());
        description.put(Constants.Features.windowType.name(), windowType);
        description.put(Constants.Features.windowPolicy.name(), windowPolicy);
        description.put(
                Constants.Features.windowLength.name(),
                windowLength); // toDo: Recalculate window length for counting windows!

        if (slidingLength == null) {
            description.put(Constants.Features.slidingLength.name(), windowLength);
        } else {
            description.put(Constants.Features.slidingLength.name(), slidingLength);
        }

        return description;
    }
}
