WITH filtered AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
    FROM shipments
    WHERE status != 'IN_TRANSIT'
)
SELECT
    id,
    carrier,
    cost,
    status,
    days_since_shipped_date,
    NTILE(10) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket,
    AVG(cost) OVER (ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3,
    FIRST_VALUE(days_since_shipped_date, TRUE) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_shipped_date,
    LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_cost
FROM filtered
WHERE days_since_shipped_date BETWEEN 54 AND 80;
