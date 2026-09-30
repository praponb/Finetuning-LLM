SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY amount DESC, id) AS event_seq, NTILE(10) OVER (ORDER BY amount DESC, id) AS amount_bucket FROM transactions;
