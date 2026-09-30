WITH valid AS (
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
    FROM shipments
    WHERE carrier IS NOT NULL
),
enriched AS (
    SELECT
        *,
        SUM(days_delivered_date_to_shipped_date) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_delivered_date_to_shipped_date,
        MIN(cost) OVER (PARTITION BY order_id) AS order_id_min_cost,
        LAST_VALUE(days_delivered_date_to_shipped_date) IGNORE NULLS OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_delivered_date_to_shipped_date
    FROM valid
),
with_windows AS (
    SELECT
        *,
        STDDEV_SAMP(cost) OVER (PARTITION BY status) AS status_stddev_samp_cost,
        LEAD(last_known_days_delivered_date_to_shipped_date, 1) OVER (PARTITION BY order_id ORDER BY order_id_min_cost, id) AS next_last_known_days_delivered_date_to_shipped_date
    FROM enriched
)
SELECT
    CASE WHEN days_delivered_date_to_shipped_date >= 80 THEN 'HIGH' ELSE 'LOW' END AS days_delivered_date_to_shipped_date_tier,
    ROUND(AVG(cost), 2) AS avg_cost,
    PERCENTILE_APPROX(order_id_min_cost, 0.5) AS order_id_min_cost_p50,
    SUM(CASE WHEN status = 'LOST' THEN running_days_delivered_date_to_shipped_date ELSE 0 END) AS lost_running_days_delivered_date_to_shipped_date
FROM with_windows
GROUP BY CASE WHEN days_delivered_date_to_shipped_date >= 80 THEN 'HIGH' ELSE 'LOW' END
ORDER BY order_id_min_cost_p50;
