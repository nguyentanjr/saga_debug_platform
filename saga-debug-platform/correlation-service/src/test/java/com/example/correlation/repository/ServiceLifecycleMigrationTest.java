package com.example.correlation.repository;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServiceLifecycleMigrationTest {
    @Test
    void aReplayedReportIsStoredOnceAndOnlyTwoStatesExist() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V19__service_lifecycle_events.sql"))
                .execute(dataSource);

        String insert = "INSERT INTO service_lifecycle_events "
                + "(service_name, state, occurred_at, received_at, source, detail, external_id) "
                + "VALUES ('inventory-service', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'docker', 'die', ?)";
        jdbc.update(insert, "DOWN", "abc:die:1");

        assertThatThrownBy(() -> jdbc.update(insert, "DOWN", "abc:die:1"))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update(insert, "PAUSED", "abc:pause:2"))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM service_lifecycle_events", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void existingRowsArePROCESSAndOnlyTwoBasesExist() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V19__service_lifecycle_events.sql"))
                .execute(dataSource);
        jdbc.update("INSERT INTO service_lifecycle_events "
                + "(service_name, state, occurred_at, received_at, source, detail, external_id) "
                + "VALUES ('inventory-service', 'DOWN', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'docker', 'die', 'a')");

        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V20__service_lifecycle_basis.sql"))
                .execute(dataSource);

        assertThat(jdbc.queryForObject("SELECT basis FROM service_lifecycle_events", String.class))
                .isEqualTo("PROCESS");
        assertThatThrownBy(() -> jdbc.update("UPDATE service_lifecycle_events SET basis = 'GUESS'"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void existingRowsStayUnscopedAfterV21() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V19__service_lifecycle_events.sql"),
                new ClassPathResource("db/migration/V20__service_lifecycle_basis.sql")).execute(dataSource);
        jdbc.update("INSERT INTO service_lifecycle_events "
                + "(service_name, state, occurred_at, received_at, source, detail, external_id) "
                + "VALUES ('payment-service', 'DOWN', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'docker', 'die', 'a')");

        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V21__service_lifecycle_scope.sql"))
                .execute(dataSource);

        assertThat(jdbc.queryForObject("SELECT scope FROM service_lifecycle_events", String.class)).isNull();
    }
}
