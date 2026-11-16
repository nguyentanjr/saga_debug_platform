package com.example.correlation.ingestion;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

public class InlineTransactionManager implements PlatformTransactionManager {
    private boolean committed;
    private boolean rolledBack;

    @Override
    public TransactionStatus getTransaction(TransactionDefinition definition) {
        return new SimpleTransactionStatus();
    }

    @Override
    public void commit(TransactionStatus status) {
        committed = true;
    }

    @Override
    public void rollback(TransactionStatus status) {
        rolledBack = true;
    }

    public boolean hasCommitted() {
        return committed;
    }

    public boolean hasRolledBack() {
        return rolledBack;
    }
}
