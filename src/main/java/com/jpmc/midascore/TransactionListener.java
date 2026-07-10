package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    @Autowired
    private DatabaseConduit databaseConduit;

    @Autowired
    private IncentiveQuerier incentiveQuerier;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        if (sender == null) {
            logger.info("Discarding transaction: sender {} not found", transaction.getSenderId());
            return;
        }

        if (recipient == null) {
            logger.info("Discarding transaction: recipient {} not found", transaction.getRecipientId());
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            logger.info("Discarding transaction: sender {} has insufficient balance", sender.getName());
            return;
        }

        Incentive incentive = incentiveQuerier.query(transaction);

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive.getAmount());

        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        databaseConduit.save(new TransactionRecord(sender, recipient, transaction.getAmount(), incentive.getAmount()));

        logger.info("Processed transaction: {} -> {} amount {} incentive {}", sender.getName(), recipient.getName(), transaction.getAmount(), incentive.getAmount());
    }
}