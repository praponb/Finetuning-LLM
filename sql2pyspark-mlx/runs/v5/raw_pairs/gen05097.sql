WITH valid AS (
    SELECT *, DATEDIFF(delivered_date, shipped_date) days_shipped_date_to_delivered_date
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'DHL'
      AND order_id IS NOT NULL
)
SELECT
    status,
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    order_id,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MIN(cost) AS min_cost,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM valid
GROUP BY status, DATE_TRUNC('MONTH', shipped_date), order_id;
