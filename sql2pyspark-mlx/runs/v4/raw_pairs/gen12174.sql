WITH latest_match AS (
    SELECT s.*, x.amount
    FROM accounts s
    JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    account_type,
    COUNT(*) FILTER (WHERE account_type = 'CHECKING') AS checking_rows,
    COUNT(*) AS row_count
FROM latest_match
GROUP BY 1
ORDER BY checking_rows;
