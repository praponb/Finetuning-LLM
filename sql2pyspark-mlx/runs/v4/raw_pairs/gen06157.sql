WITH cleaned AS (
    SELECT
        *,
        COALESCE(name, 'unassigned') AS name_filled,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM customers
    WHERE signup_date > DATE '2023-04-02'
      AND segment IS NOT NULL
)
SELECT s.*, x.opened_date
FROM cleaned s
JOIN LATERAL (
        SELECT c.opened_date
        FROM accounts c
        WHERE c.customer_id = s.id
        ORDER BY c.balance DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
