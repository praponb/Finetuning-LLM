SELECT a.*, d.segment
FROM accounts a
INNER JOIN customers d
    ON a.customer_id <=> d.id
