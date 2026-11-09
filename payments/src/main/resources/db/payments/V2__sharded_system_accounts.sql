-- Every authorisation and capture posts to a holds account, and every payout to a clearing account: one
-- of each per currency made them the hottest rows in the ledger (each posting locks its accounts' rows).
-- Now there are several of each, and each payment or payout keeps the one it was given. A row from before
-- this has none, and keeps the original account (payments:holds:<currency>, payments:clearing:<currency>).
ALTER TABLE payments ADD COLUMN holds_account uuid;
ALTER TABLE payouts ADD COLUMN clearing_account uuid;
