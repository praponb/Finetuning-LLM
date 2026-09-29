WITH base AS (
    SELECT
        *,
        DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_date_label,
        CONCAT(status, ' - ', carrier) AS status_carrier
    FROM shipments
    WHERE delivered_date > DATE '2023-10-04'
      AND carrier IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        CUME_DIST() OVER (ORDER BY cost) AS cost_cume_dist,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        CUME_DIST() OVER (ORDER BY cost) AS cost_cume_dist_2,
        LAST_VALUE(carrier) IGNORE NULLS OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier
    FROM base
    WHERE MONTH(delivered_date) IN (1, 8, 12)
),
scored AS (
    SELECT
        *,
        LAG(cost_cume_dist, 1) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS prev_cost_cume_dist,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROUND(running_cost / SUM(running_cost) OVER (PARTITION BY status, carrier), 4) AS running_cost_share,
        LAST_VALUE(cost_share, TRUE) OVER w_hist AS last_known_cost_share,
        FIRST_VALUE(cost_cume_dist, TRUE) OVER (ORDER BY cost_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_cume_dist
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY status, carrier ORDER BY delivered_date, id), w_hist AS (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_shipments AS (
    SELECT
        *,
        MIN(first_known_cost_cume_dist) OVER w_ordered AS order_id_min_first_known_cost_cume_dist,
        LAG(cost_cume_dist, 2) OVER (PARTITION BY status ORDER BY running_cost_share, id) AS prev_cost_cume_dist_2,
        FIRST_VALUE(prev_cost_cume_dist) IGNORE NULLS OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_cost_cume_dist,
        LAST_VALUE(cost_cume_dist, TRUE) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_cume_dist
    FROM scored
    WINDOW w_ordered AS (PARTITION BY order_id)
),
enriched AS (
    SELECT
        a.*,
        b.amount,
        b.channel,
        b.status AS order_status
    FROM with_windows_shipments a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        delivered_date_label,
        cost_share,
        running_cost,
        cost_cume_dist_2,
        event_seq,
        running_cost_share,
        order_id_min_first_known_cost_cume_dist,
        prev_cost_cume_dist_2,
        first_known_prev_cost_cume_dist,
        last_known_cost_cume_dist,
        amount,
        channel,
        order_status,
        prev_cost_cume_dist * running_cost AS prev_cost_cume_dist_times_running_cost,
        COALESCE(order_status, 'OTHER') AS resolved_order_status
    FROM enriched
    WHERE order_id IS NOT NULL
      AND order_id IN (26, 37)
)
SELECT
    CASE WHEN last_known_cost_cume_dist >= 0.77 THEN 'LARGE' ELSE 'SMALL' END AS last_known_cost_cume_dist_tier,
    COUNT(*) FILTER (WHERE carrier = 'Flash') AS flash_rows,
    ROUND(SUM(cost_cume_dist_2) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS cost_cume_dist_2_per_partner,
    MAX(last_known_cost_cume_dist) AS max_last_known_cost_cume_dist,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) FILTER (WHERE order_status = 'RETURNED') AS returned_rows
FROM cleaned
GROUP BY 1
HAVING ROUND(SUM(cost_cume_dist_2) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) < 1000000
ORDER BY max_last_known_cost_cume_dist DESC NULLS FIRST
