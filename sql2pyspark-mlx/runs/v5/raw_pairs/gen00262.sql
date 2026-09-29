WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(delivered_date, 1) AS delivered_date_plus_months
    FROM shipments
    WHERE shipped_date BETWEEN '2023-12-11' AND '2025-07-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cost) OVER w_grp AS prev_cost,
        LEAD(cost) OVER w_grp AS next_cost,
        ROW_NUMBER() OVER w_grp AS event_seq,
        AVG(cost) OVER w_running AS cost_avg_5
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY carrier ORDER BY cost, id), w_running AS (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.delivered_date,
        e.prev_cost,
        e.next_cost,
        b.channel,
        b.shipping_fee,
        b.status AS order_status
    FROM scored e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT order_id, id
    FROM enriched
    INTERSECT
    SELECT order_id, id
    FROM shipments
    WHERE shipped_date BETWEEN '2023-10-01' AND '2025-01-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM kept_rows s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.delivered_date BETWEEN pr.starts_on AND pr.ends_on
),
with_window_shipments AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_window e
    JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.delivered_date >= pr.starts_on
       AND e.delivered_date <= DATE_ADD(pr.ends_on, 10)
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        order_status,
        promo_id,
        promo_code,
        discount_pct,
        promo_id_2,
        promotion_discount_pct,
        PERCENT_RANK() OVER (PARTITION BY channel, order_id ORDER BY discount_pct DESC NULLS FIRST) AS discount_pct_percent_rank,
        RANK() OVER (ORDER BY shipping_fee DESC NULLS FIRST) AS shipping_fee_rank,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY order_status), 4) AS promo_id_share
    FROM with_window_shipments
    WHERE order_id = 8
)
-- Grouped metrics
SELECT
    order_id,
    promo_id,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    COUNT(*) FILTER (WHERE order_status = 'COMPLETED') AS completed_rows,
    SUM(promo_id_share) FILTER (WHERE order_status = 'RETURNED') AS returned_total_promo_id_share,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(discount_pct, 0.95) AS discount_pct_p95,
    APPROX_PERCENTILE(promotion_discount_pct, 0.5) AS promotion_discount_pct_p50
FROM scored_shipments
GROUP BY order_id, promo_id;
