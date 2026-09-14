 Monitoramento de Vendas e Logística de um E-commerce

Projeto desenvolvido para a disciplina de Big Data, com o objetivo de construir uma arquitetura de processamento de dados que combine **streaming em tempo real** e **processamento em lote (batch)**.
O sistema simula eventos de um e-commerce, como cliques, ações no carrinho, compras e entregas, utilizando diferentes tecnologias do ecossistema Big Data.

Objetivo:

Construir um pipeline de dados capaz de:

- Gerar eventos de um e-commerce continuamente;
- Capturar e ingerir os eventos utilizando Apache Flume;
- Armazenar os dados brutos no HDFS;
- Processar eventos em tempo real com Apache Flink;
- Utilizar janelas deslizantes e Watermarks;
- Gerar alertas em tempo real e armazená-los no HBase;
- Processar o histórico utilizando Apache Spark;
- Realizar processos de ETL;
- Demonstrar o uso de Wide Dependencies;
- Consolidar os dados no Hive;
- Disponibilizar uma interface de monitoramento através do frontend.

