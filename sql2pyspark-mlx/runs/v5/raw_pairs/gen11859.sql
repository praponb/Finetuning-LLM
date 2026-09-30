WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(carrier) AS carrier_upper,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.delivered_date,
        src.cost,
        src.status,
        src.carrier_clean,
        d.status AS order_status,
        d.channel,
        d.order_date
    FROM cleaned src
    LEFT JOIN orders d
        ON src.order_id = d.id
    WHERE d.customer_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        cost,
        status,
        carrier_clean,
        order_status,
        channel,
        order_date,
        LEAD(cost) OVER w_running AS next_cost,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share
    FROM with_dims
    WINDOW w_running AS (PARTITION BY order_id ORDER BY order_date, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(next_cost) OVER (PARTITION BY order_status, channel ORDER BY next_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_next_cost,
        SUM(next_cost) OVER (PARTITION BY order_status, channel ORDER BY next_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_next_cost,
        AVG(next_cost) OVER (PARTITION BY order_status, channel ORDER BY next_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_next_cost,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY cost_share DESC, id) AS row_seq
    FROM scored
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_windows src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date <= DATE_ADD(pr.ends_on, 10)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        cost,
        carrier_clean,
        order_status,
        last4_count_next_cost,
        last4_sum_next_cost,
        last4_avg_next_cost,
        row_seq,
        promo_id,
        promo_code,
        DENSE_RANK() OVER (PARTITION BY order_status ORDER BY next_cost NULLS FIRST) AS next_cost_dense_rank,
        NTILE(10) OVER w_part AS event_seq_bucket,
        SUM(cost_share) OVER (PARTITION BY status) AS status_sum_cost_share,
        ROW_NUMBER() OVER (PARTITION BY channel, order_status ORDER BY row_seq, id, promo_id) AS seq_no
    FROM attributed
    WINDOW w_part AS (PARTITION BY status ORDER BY event_seq DESC, id, promo_id)
),
scored_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY promo_code ORDER BY last4_count_next_cost, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        AVG(last4_sum_next_cost) OVER (PARTITION BY order_status ORDER BY shipped_date, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_next_cost_avg_3
    FROM metrics
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) AS completed_count,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    SIZE(COLLECT_SET(order_status)) AS num_order_status,
    AVG(last4_avg_next_cost) AS avg_last4_avg_next_cost,
    PERCENTILE_APPROX(row_seq, 0.9) AS row_seq_p90,
    ROUND(SUM(last4_count_next_cost) / NULLIF(COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END), 0), 2) AS last4_count_next_cost_per_vip
FROM scored_shipments
GROUP BY year_start
HAVING ROUND(AVG(seq_no), 2) > 0;
