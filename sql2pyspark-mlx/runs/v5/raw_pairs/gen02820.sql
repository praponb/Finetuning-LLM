SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY amount, id) AS event_seq,
    PERCENT_RANK() OVER (ORDER BY amount DESC) AS amount_percent_rank
FROM transactions;
