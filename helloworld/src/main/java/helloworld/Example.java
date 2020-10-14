package helloworld;

import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadLocalRandom;

public class Example {

    private static final FunctionType GREET = new FunctionType("example", "greet");
    private static final EgressIdentifier<String> GREETINGS =
            new EgressIdentifier<>("example", "out", String.class);

    public static void main(String... args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

        DataStreamSource<String> ingress = env.addSource(new NameSource());

        DataStream<RoutableMessage> router =
                ingress.map(name ->
                        RoutableMessageBuilder.builder()
                                .withTargetAddress(GREET, name)
                                .withMessageBody(name)
                                .build());

        StatefulFunctionEgressStreams statefulfunction =
                StatefulFunctionDataStreamBuilder.builder("example")
                        .withDataStreamAsIngress(router)
                        .withFunctionProvider(GREET, unused -> new MyFunction())
                        .withEgressId(GREETINGS)
                        .withConfiguration(statefunConfig)
                        .build(env);

        DataStream<String> egress = statefulfunction.getDataStreamForEgressId(GREETINGS);

        egress.print();

        env.execute();
    }

    private static final class MyFunction implements StatefulFunction {

        @Persisted
        private final PersistedValue<Integer> seenCount = PersistedValue.of("seen", Integer.class);

        @Override
        public void invoke(Context context, Object input) {
            int seen = seenCount.updateAndGet(MyFunction::increment);
            context.send(GREETINGS, String.format("Hello %s at the %d-th time", input, seen));
        }

        private static int increment(@Nullable Integer n) {
            return n == null ? 1 : n + 1;
        }
    }

    private static final class NameSource implements SourceFunction<String> {

        private volatile boolean canceled;

        @Override
        public void run(SourceContext<String> ctx) throws InterruptedException {
            String[] names = {"Stephan", "Igal", "Gordon", "Seth", "Marta"};
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (!canceled) {
                final String name = names[random.nextInt(names.length)];
                ctx.collect(name);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }
    }
}
