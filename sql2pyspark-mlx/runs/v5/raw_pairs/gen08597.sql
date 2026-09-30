WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        c.segment,
        s.status,
        COUNT(*) AS num_rows,
        MIN(s.monthly_fee) AS min_monthly_fee
    FROM subscriptions s
    INNER JOIN customers c ON s.customer_id = c.id
    GROUP BY CUBE (c.segment, s.status)
)
SELECT
    segment,
    status,
    min_monthly_fee
FROM rollup_totals
