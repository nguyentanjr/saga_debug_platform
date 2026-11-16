package com.example.correlation.repository;

import com.example.correlation.domain.ExecutionAlias;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ExecutionAliasRepository
        extends JpaRepository<ExecutionAlias, ExecutionAlias.Key> {
    List<ExecutionAlias> findByExecutionId(String executionId);
}
