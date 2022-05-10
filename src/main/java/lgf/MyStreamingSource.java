package lgf;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author zhuang.ma
 * @date 2022/5/6
 */
public class MyStreamingSource implements SourceFunction<Item> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            Item item = generateItem();
            ctx.collect(item);
            Thread.sleep(1000);
        }
    }

    private Item generateItem() {
        int i = new Random().nextInt(100);
        return new Item("name" + i, i + "");
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
