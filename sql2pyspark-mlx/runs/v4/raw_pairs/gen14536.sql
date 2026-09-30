WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY amount NULLS LAST, id) AS rank_in_group
        FROM payments
    ) ranked
    WHERE rank_in_group = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        status,
        rank_in_group,
        HOUR(paid_at) AS paid_at_hour
    FROM deduped
    WHERE method IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY rank_in_group DESC, id) AS rank_in_group_2
    FROM filtered
    QUALIFY rank_in_group_2 = 1
),
deduped_payments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY paid_at_hour DESC NULLS FIRST) AS rn
    FROM latest
    QUALIFY rn = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.channel,
        p.shipping_fee,
        p.order_date
    FROM deduped_payments s
    JOIN orders p
        ON s.order_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    channel,
    CASE WHEN paid_at_hour >= 77 THEN 'HIGH' ELSE 'LOW' END AS paid_at_hour_tier,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    MAX(shipping_fee) FILTER (WHERE status = 'SUCCESS') AS success_max_shipping_fee,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', order_date) ORDER BY ROUND(AVG(shipping_fee), 2) DESC) AS avg_shipping_fee_dense_rank
FROM enriched
GROUP BY DATE_TRUNC('WEEK', order_date), channel, CASE WHEN paid_at_hour >= 77 THEN 'HIGH' ELSE 'LOW' END
HAVING avg_shipping_fee >= 0;
