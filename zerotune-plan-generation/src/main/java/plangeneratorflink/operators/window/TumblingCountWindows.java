package plangeneratorflink.operators.window;

import java.util.Collection;
import java.util.Collections;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class TumblingCountWindows extends WindowAssigner<Object, GlobalWindow> {
    private static final long serialVersionUID = 1L;

    private final Integer size;

    protected TumblingCountWindows(Integer size) {
        if (size < 0) {
            throw new IllegalArgumentException(
                    "TumblingCountWindows parameters must satisfy size >= 0");
        }

        this.size = size;
    }

    @Override
    public String toString() {
        return "TumblingCountWindows(" + size + ")";
    }

    /**
     * Creates a new {@code TumblingCountWindows} {@link WindowAssigner} that
     * assigns elements
     * to count windows based on the incoming number of elements.
     *
     * @param size The size of the generated windows.
     * @return The count policy.
     */
    public static TumblingCountWindows of(Integer size) {
        return new TumblingCountWindows(size);
    }

    @Override
    public TypeSerializer<GlobalWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new GlobalWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    @Override
    public Collection<GlobalWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        return Collections.singletonList(GlobalWindow.get());
    }

    @Override
    public Trigger<Object, GlobalWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return PurgingTrigger.of(CountTrigger.of(size));
    }
}
