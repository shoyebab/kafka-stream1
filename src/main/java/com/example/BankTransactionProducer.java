package com.example;

import com.example.model.JsonSerde;
import com.example.model.BankTransaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;


import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class BankTransactionProducer {

    public static void main(String[] args) {
        // Create producer with Long key and BankTransaction value
        KafkaProducer<Long, BankTransaction> bankTransactionProducer =
                new KafkaProducer<>(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerde.class.getName()
                ), new LongSerializer(), new JsonSerde<>(BankTransaction.class).serializer());

        // Sample transactions
        List<BankTransaction> data1 = List.of(
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(500))
                        .build(),
                BankTransaction.builder()
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(3000)).build(),
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(500)).build(),
                BankTransaction.builder()
                        .balanceId(4L)
                        .time(new Date())
                        .amount(new BigDecimal(2000)).build(),
                BankTransaction.builder()
                        .balanceId(4L)
                        .time(new Date())
                        .amount(new BigDecimal(-2500)).build(),
                BankTransaction.builder()
                        .balanceId(3L)
                        .time(new Date())
                        .amount(new BigDecimal(1000)).build(),
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(-500)).build(),
                BankTransaction.builder()
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(-4000)).build(),
                BankTransaction.builder()
                        .balanceId(3L)
                        .time(new Date())
                        .amount(new BigDecimal(-500)).build()
        );

        // Send transactions
        data1.forEach(bankTransaction ->
                send(bankTransactionProducer,
                        new ProducerRecord<>("bank-transactions", bankTransaction.getBalanceId(), bankTransaction)));

        // Extra test transaction
        BankTransaction bankTransaction = BankTransaction.builder()
                .balanceId(3L)
                .time(new Date())
                .amount(new BigDecimal(-10_000)).build();

        send(bankTransactionProducer, new ProducerRecord<>("bank-transactions", bankTransaction.getBalanceId(), bankTransaction));

        bankTransactionProducer.close();
    }

    private static void send(KafkaProducer<Long, BankTransaction> producer, ProducerRecord<Long, BankTransaction> record) {
        try {
            producer.send(record).get(); // synchronous for demo
            System.out.printf("Sent transaction: %s%n", record.value());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
