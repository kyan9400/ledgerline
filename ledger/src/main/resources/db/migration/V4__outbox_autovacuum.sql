-- The outbox is a queue: every row is deleted within moments of being written. Vacuum it after every
-- thousand or so deletions, not after a fifth of the table, so its pages are reused and the relay's
-- reads stay small.
ALTER TABLE outbox SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000,
                        autovacuum_vacuum_cost_delay = 0, fillfactor = 100);
