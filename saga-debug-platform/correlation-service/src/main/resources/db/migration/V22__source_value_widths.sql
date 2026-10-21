-- Columns holding values the observed system chooses, still sized for this platform's demo.
--
-- V12 widened the identifier columns for the same reason; these were left at the widths
-- V1 picked for ORDER_CREATED-style names. A source event type that no mapping translates
-- is stored as the source wrote it, and for Java systems that is a fully qualified class
-- name: FTGO's "net.chrisrichardson.ftgo.orderservice.api.events.OrderCreatedEvent" is 66
-- characters. The insert failed with 22001, the record was retried until given up, and every
-- observation behind it on the partition waited (ROADMAP EXT-LONG-TYPE). An untranslated
-- type is ordinary on real systems, not a broken declaration.
--
-- Widening only: no existing value changes, and VARCHAR(n) -> VARCHAR(m>n) is a catalog
-- change in Postgres, not a table rewrite.
ALTER TABLE saga_events    ALTER COLUMN event_type    TYPE VARCHAR(255);
ALTER TABLE saga_events    ALTER COLUMN step_name     TYPE VARCHAR(255);
ALTER TABLE saga_events    ALTER COLUMN service_name  TYPE VARCHAR(255);
ALTER TABLE saga_events    ALTER COLUMN causation_id  TYPE VARCHAR(300);
ALTER TABLE saga_instances ALTER COLUMN workflow_type TYPE VARCHAR(255);
ALTER TABLE saga_instances ALTER COLUMN order_id      TYPE VARCHAR(300);
