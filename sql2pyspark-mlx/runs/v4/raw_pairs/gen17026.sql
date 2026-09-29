WITH matched_keys AS (
    SELECT status
    FROM accounts
    INTERSECT ALL
    SELECT status
    FROM accounts
    WHERE customer_id IN (2, 17, 20)
),
kept_rows AS (
    SELECT s.*
    FROM accounts s
    INNER JOIN matched_keys k ON s.status = k.status
)
SELECT
    id,
    customer_id,
    account_type,
    opened_date
FROM kept_rows;
