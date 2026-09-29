SELECT
    t.id,
    t.account_id,
    t.amount,
    t.is_flagged
FROM transactions t
WHERE t.account_id IN (17, 26, 37) AND t.account_id NOT IN (SELECT id FROM accounts WHERE balance >= 58000);
