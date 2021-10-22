package bigdata.clebeg.cn.quickstart.window;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import java.util.Collection;
import java.util.Collections;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer;

public class DayWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext windowAssignerContext) {
        DateTime eleDT = DateUtil.date(timestamp);
        long beginTime = DateUtil.beginOfDay(eleDT).getTime();
        long endTime = DateUtil.beginOfDay(DateUtil.offsetDay(eleDT, 1)).getTime();
        return Collections.singletonList(new TimeWindow(beginTime, endTime));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
