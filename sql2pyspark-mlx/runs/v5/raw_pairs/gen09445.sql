WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT id, order_id
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        d.amount
    FROM deduped src
    INNER JOIN orders d
        ON src.order_id = d.id
    WHERE d.store_id IN (10, 23, 32)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        CAST(amount AS INT) AS amount_int
    FROM enriched
    WHERE amount BETWEEN 720 AND 900
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM order_items
    WHERE quantity > 6
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(amount_filled) AS amount_filled_whole
    FROM reconciled
    WHERE amount > 890
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.order_id,
        s.amount,
        s.amount_filled,
        s.amount_int,
        p.shipping_fee,
        p.discount
    FROM valid s
    LEFT JOIN orders p
        ON s.order_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount_filled >= 80 THEN 'LARGE' ELSE 'SMALL' END AS amount_filled_tier,
    APPROX_PERCENTILE(shipping_fee, 0.95) AS shipping_fee_p95,
    PERCENTILE_APPROX(shipping_fee, 0.95) AS shipping_fee_p95_2,
    COUNT(*) AS row_count,
    AVG(amount_int) AS avg_amount_int,
    PERCENTILE_APPROX(discount, 0.95) AS discount_p95
FROM with_dims
GROUP BY 1;
