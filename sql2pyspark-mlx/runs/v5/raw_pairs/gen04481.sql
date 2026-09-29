WITH kept_keys AS (
    -- Set difference on keys
    SELECT status, channel
    FROM orders
    EXCEPT DISTINCT
    SELECT status, channel
    FROM orders
    WHERE store_id IN (3, 6)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM orders s
    INNER JOIN kept_keys k ON s.status = k.status AND s.channel = k.channel
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        order_date,
        shipping_fee,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) AS shipping_fee_dense_rank,
        CUME_DIST() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST) AS amount_cume_dist,
        CORR(shipping_fee, amount) OVER (PARTITION BY customer_id) AS corr_shipping_fee_amount
    FROM reconciled
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.order_date,
        a.shipping_fee,
        a.shipping_fee_dense_rank,
        a.amount_cume_dist,
        a.corr_shipping_fee_amount,
        p.name,
        p.country,
        p.segment
    FROM with_windows a
    INNER JOIN customers p
        ON a.customer_id <=> p.id
),
enriched_orders AS (
    -- Window metrics over several different partitions
    SELECT
        order_date,
        shipping_fee,
        corr_shipping_fee_amount,
        segment,
        COUNT(shipping_fee_dense_rank) OVER (PARTITION BY segment, country) AS segment_count_shipping_fee_dense_rank,
        SUM(amount_cume_dist) OVER (PARTITION BY country) AS country_sum_amount_cume_dist
    FROM enriched
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        order_date,
        shipping_fee,
        corr_shipping_fee_amount,
        segment,
        country_sum_amount_cume_dist,
        ROUND(shipping_fee * 1.15, 2) AS adjusted_shipping_fee,
        shipping_fee + segment_count_shipping_fee_dense_rank AS shipping_fee_plus_segment_count_shipping_fee_dense_rank,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM enriched_orders
    WHERE order_date >= DATE '2023-08-22'
      AND country_sum_amount_cume_dist < 2
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    PERCENTILE_APPROX(shipping_fee_plus_segment_count_shipping_fee_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_plus_segment_count_shipping_fee_dense_rank_quartiles,
    ROUND(SUM(shipping_fee_plus_segment_count_shipping_fee_dense_rank) / NULLIF(COUNT(CASE WHEN segment = 'Corporate' THEN 1 END), 0), 2) AS shipping_fee_plus_segment_count_shipping_fee_dense_rank_per_corporate,
    AVG(adjusted_shipping_fee) AS avg_adjusted_shipping_fee,
    APPROX_PERCENTILE(shipping_fee, 0.5) AS shipping_fee_p50
FROM cleaned
GROUP BY DATE_TRUNC('WEEK', order_date)
HAVING AVG(adjusted_shipping_fee) > 0;
