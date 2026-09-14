package com.ecommerce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.UUID;

public class FlinkMonitoramentoJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> linhas =
                env.addSource(new EventFileSource("/input/events.log"));

        ObjectMapper mapper = new ObjectMapper();

        DataStream<Evento> eventos = linhas
                .map(new MapFunction<String, Evento>() {
                    @Override
                    public Evento map(String linha) throws Exception {
                        return mapper.readValue(linha, Evento.class);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Evento>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        (evento, timestamp) ->
                                                evento.getTimestampMillis()
                                )
                );

        DataStream<Alerta> alertas = eventos
                .keyBy(Evento::getEvent_type)
                .window(
                        SlidingEventTimeWindows.of(
                                Time.seconds(30),
                                Time.seconds(10)
                        )
                )
                .process(
                        new ProcessWindowFunction<
                                Evento,
                                Alerta,
                                String,
                                TimeWindow>() {

                            @Override
                            public void process(
                                    String tipoEvento,
                                    Context contexto,
                                    Iterable<Evento> elementos,
                                    Collector<Alerta> out) {

                                int quantidade = 0;

                                for (Evento evento : elementos) {
                                    quantidade++;
                                }

                                if ("carrinho".equals(tipoEvento)
                                        && quantidade >= 3) {

                                    Alerta alerta = new Alerta(
                                            tipoEvento,
                                            quantidade,
                                            contexto.window().getStart(),
                                            contexto.window().getEnd()
                                    );

                                    out.collect(alerta);
                                }
                            }
                        }
                );

        alertas.print();

        alertas.addSink(new HBaseAlertSink());

        env.execute("Monitoramento de Vendas e Logistica");
    }

    public static class EventFileSource
            extends org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction<String> {

        private final String caminhoArquivo;
        private volatile boolean executando = true;

        public EventFileSource(String caminhoArquivo) {
            this.caminhoArquivo = caminhoArquivo;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            try (RandomAccessFile arquivo =
                         new RandomAccessFile(caminhoArquivo, "r")) {

                arquivo.seek(arquivo.length());

                while (executando) {

                    String linha = arquivo.readLine();

                    if (linha != null) {

                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(linha);
                        }

                    } else {
                        Thread.sleep(1000);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            executando = false;
        }
    }

    public static class Alerta {

        private String evento;
        private int quantidade;
        private long janelaInicio;
        private long janelaFim;

        public Alerta() {
        }

        public Alerta(
                String evento,
                int quantidade,
                long janelaInicio,
                long janelaFim) {

            this.evento = evento;
            this.quantidade = quantidade;
            this.janelaInicio = janelaInicio;
            this.janelaFim = janelaFim;
        }

        public String getEvento() {
            return evento;
        }

        public int getQuantidade() {
            return quantidade;
        }

        public long getJanelaInicio() {
            return janelaInicio;
        }

        public long getJanelaFim() {
            return janelaFim;
        }

        @Override
        public String toString() {
            return "ALERTA | evento=" + evento +
                    " | quantidade=" + quantidade +
                    " | janela_inicio=" + janelaInicio +
                    " | janela_fim=" + janelaFim;
        }
    }

    public static class HBaseAlertSink
            extends RichSinkFunction<Alerta> {

        private Connection connection;
        private Table table;

        @Override
        public void open(
                org.apache.flink.configuration.Configuration parameters)
                throws Exception {

            Configuration config =
                    HBaseConfiguration.create();

            config.set(
                    "hbase.zookeeper.quorum",
                    "hbase"
            );

            config.set(
                    "hbase.zookeeper.property.clientPort",
                    "2181"
            );

            connection =
                    ConnectionFactory.createConnection(config);

            table =
                    connection.getTable(
                            TableName.valueOf("alertas")
                    );
        }

        @Override
        public void invoke(
                Alerta alerta,
                Context context)
                throws Exception {

            String rowKey =
                    System.currentTimeMillis()
                            + "-"
                            + UUID.randomUUID();

            Put put = new Put(
                    Bytes.toBytes(rowKey)
            );

            put.addColumn(
                    Bytes.toBytes("dados"),
                    Bytes.toBytes("evento"),
                    Bytes.toBytes(alerta.getEvento())
            );

            put.addColumn(
                    Bytes.toBytes("dados"),
                    Bytes.toBytes("quantidade"),
                    Bytes.toBytes(alerta.getQuantidade())
            );

            put.addColumn(
                    Bytes.toBytes("dados"),
                    Bytes.toBytes("janela_inicio"),
                    Bytes.toBytes(alerta.getJanelaInicio())
            );

            put.addColumn(
                    Bytes.toBytes("dados"),
                    Bytes.toBytes("janela_fim"),
                    Bytes.toBytes(alerta.getJanelaFim())
            );

            put.addColumn(
                    Bytes.toBytes("dados"),
                    Bytes.toBytes("mensagem"),
                    Bytes.toBytes(alerta.toString())
            );

            table.put(put);
        }

        @Override
        public void close() throws Exception {

            if (table != null) {
                table.close();
            }

            if (connection != null) {
                connection.close();
            }
        }
    }
}