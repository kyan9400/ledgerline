-- The fraud scorer's verdict on a card payment, asked once, before its money moves (see FraudCheck).
--   fraud_decision: approve, decline, or unscored (the scorer didn't answer in time: the payment goes on)
ALTER TABLE payments
    ADD COLUMN fraud_score double precision,
    ADD COLUMN fraud_decision text CHECK (fraud_decision IN ('approve', 'decline', 'unscored')),
    ADD COLUMN fraud_model text,
    ADD COLUMN fraud_reasons text;
