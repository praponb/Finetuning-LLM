WITH enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.shipping_fee,
        d.channel,
        d.order_date
    FROM payments e
    JOIN orders d
        ON e.order_id = d.id
    WHERE d.amount <= 780
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status
    FROM enriched
    INTERSECT ALL
    SELECT status
    FROM payments
    WHERE order_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.status = k.status
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN shipping_fee >= 38 THEN 'High' WHEN shipping_fee >= 8 THEN 'Medium' ELSE 'Low' END shipping_fee_level
    FROM verified
    WHERE channel IS NOT NULL
      AND channel IS DISTINCT FROM 'web'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM valid e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.paid_at >= pr.starts_on
       AND e.paid_at <= DATE_ADD(pr.ends_on, 3)
),
enriched_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(shipping_fee) OVER (PARTITION BY status) status_max_shipping_fee,
        CUME_DIST() OVER (ORDER BY discount_pct NULLS LAST) discount_pct_cume_dist
    FROM matched
)
SELECT
    id,
    method,
    status,
    shipping_fee,
    discount_pct,
    discount_pct_cume_dist
FROM enriched_payments
ORDER BY discount_pct_cume_dist DESC
