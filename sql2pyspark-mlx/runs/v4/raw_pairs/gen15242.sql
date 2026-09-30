SELECT
    id,
    paid_at,
    amount,
    INITCAP(TRIM(method)) AS method_title,
    SUBSTRING(method, 1, 3) AS method_prefix
FROM payments
