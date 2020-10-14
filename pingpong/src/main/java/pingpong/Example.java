package pingpong;

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

public class Example {

    private static final FunctionType PINGERPONGER = new FunctionType("example", "pingerponger");
    private static final EgressIdentifier<String> GREETINGS =
            new EgressIdentifier<>("example", "out", String.class);

    public static void main(String... args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

        DataStreamSource<Integer> ingress = env.fromElements(10000000);

        DataStream<RoutableMessage> router =
                ingress.map(c ->
                        RoutableMessageBuilder.builder()
                                .withTargetAddress(PINGERPONGER, "pinger")
                                .withMessageBody(c)
                                .build());

        StatefulFunctionEgressStreams statefulfunction =
                StatefulFunctionDataStreamBuilder.builder("example")
                        .withDataStreamAsIngress(router)
                        .withFunctionProvider(PINGERPONGER, unused -> new PingerPonger())
                        .withEgressId(GREETINGS)
                        .withConfiguration(statefunConfig)
                        .build(env);

        DataStream<String> egress = statefulfunction.getDataStreamForEgressId(GREETINGS);

        egress.print();

        env.execute();
    }

    private static final class PingerPonger implements StatefulFunction {
        @Persisted
        private final PersistedValue<Integer> count = PersistedValue.of("seen", Integer.class);
        private final PersistedValue<Long> time = PersistedValue.of("seen", Long.class);

        @Override
        public void invoke(Context context, Object input) {
            int c = (int) input - 1;
            int seen = count.getOrDefault(0) + 1;
            long t = time.getOrDefault(System.currentTimeMillis());
            count.set(seen);
            time.set(t);
            if (seen % (1 << 20) == 0 & context.self().id().equals("pinger")) {
                double txs = 1000.d * (double) seen / (double) (System.currentTimeMillis() - t);
                context.send(GREETINGS, String.format("pingponged %d times, %d left to pingpong, %f tx per second ", seen, c, txs));
            }
            if (c == 0) {
                Long td = System.currentTimeMillis() - t;
                context.send(GREETINGS, String.format("pingponged %d times, %d left to pingpong, %d ms total time", seen, c, td));
            }
            if (c > 0) {
                if (context.self().id().equals("pinger")) {
                    context.send(PINGERPONGER, "ponger", c);
                } else if (context.self().id().equals("ponger")) {
                    context.send(PINGERPONGER, "pinger", c);
                }
            }
        }
    }
}
