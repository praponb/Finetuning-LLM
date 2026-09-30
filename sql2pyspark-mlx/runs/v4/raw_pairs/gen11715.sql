WITH scored AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY shipped_date DESC, id) AS seq_no
    FROM shipments
)
SELECT
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    PERCENTILE_APPROX(cost, 0.9) AS cost_p90,
    MAX(cost) FILTER (WHERE status = 'RETURNED') AS returned_max_cost,
    MIN(next_cost) AS min_next_cost,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', delivered_date) ORDER BY PERCENTILE_APPROX(cost, 0.9) DESC) AS cost_p90_percent_rank
FROM scored
GROUP BY 1, 2;
