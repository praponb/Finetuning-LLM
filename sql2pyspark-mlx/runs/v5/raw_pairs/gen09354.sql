SELECT
    id,
    account_id,
    amount,
    MAX(amount) OVER (PARTITION BY account_id) AS account_id_max_amount
FROM transactions;
