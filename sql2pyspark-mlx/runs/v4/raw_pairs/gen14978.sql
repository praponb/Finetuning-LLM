WITH enriched AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        COUNT(cost) OVER w_ordered AS status_count_cost,
        COUNT(cost) OVER w_ordered AS status_count_cost_2
    FROM shipments
    WINDOW w_ordered AS (PARTITION BY status)
)
SELECT
    CASE WHEN status_count_cost_2 >= 15 THEN 'HIGH' ELSE 'LOW' END AS status_count_cost_2_tier,
    order_id,
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(status_count_cost), 2) AS avg_status_count_cost,
    COUNT(*) AS num_rows,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN status_count_cost_2 >= 15 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(*) DESC) AS num_rows_percent_rank
FROM enriched
GROUP BY status_count_cost_2_tier, order_id, activity_month
