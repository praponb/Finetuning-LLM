WITH base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.product_id,
        e.quantity,
        e.unit_price,
        e.unit_price_filled,
        p.order_date,
        p.discount,
        p.status
    FROM base e
    INNER JOIN orders p
        ON e.order_id = p.id
    WHERE p.customer_id IS NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.shipping_fee,
        d.order_date AS order_order_date
    FROM enriched e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT product_id, order_id
    FROM with_dims
    EXCEPT
    SELECT product_id, order_id
    FROM order_items
    WHERE unit_price BETWEEN 210 AND 380
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER w_grp AS discount_percent_rank,
        RANK() OVER (PARTITION BY order_id, product_id ORDER BY shipping_fee NULLS FIRST) AS shipping_fee_rank
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY discount DESC NULLS LAST)
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT
    SELECT id
    FROM order_items
    WHERE product_id IN (13, 33)
),
reconciled_order_items AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys_2 k ON s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY discount NULLS LAST) AS discount_percent_rank_2
    FROM reconciled_order_items
)
SELECT
    id,
    order_id,
    product_id,
    order_date,
    discount,
    status,
    shipping_fee,
    order_order_date,
    discount_percent_rank_2,
    CASE WHEN status RLIKE '^(CANCELLED|COMPLETED)$' THEN 'CORE' ELSE 'GROUP_B' END AS status_zone
FROM with_windows
ORDER BY product_id;
