WITH cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        DATE_SUB(delivered_date, 82) AS delivered_date_minus_82d,
        DATE_FORMAT(shipped_date, 'MMM yyyy') AS shipped_date_label,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM shipments
    WHERE delivered_date > TO_DATE('2024-03-16')
      AND carrier IS NOT NULL
),
metrics AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        delivered_date_minus_82d,
        shipped_date_label,
        resolved_carrier,
        AVG(cost) OVER w_recent AS cost_avg_6_prior,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        LAST_VALUE(carrier, TRUE) OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY cost, id) AS cost_bucket
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
with_windows AS (
    SELECT
        *,
        AVG(cost_avg_6) OVER (PARTITION BY order_id) AS order_id_avg_cost_avg_6,
        SUM(cost_avg_6_prior) OVER (PARTITION BY carrier, order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_avg_6_prior,
        SUM(cost_avg_6) OVER (ORDER BY cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_avg_6
    FROM metrics
)
SELECT
    id,
    cost,
    status,
    shipped_date_label,
    resolved_carrier,
    cost_avg_6,
    last_known_carrier,
    order_id_avg_cost_avg_6,
    cumulative_cost_avg_6_prior
FROM with_windows
ORDER BY cumulative_cost_avg_6_prior DESC, status DESC;
