WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        discount,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM orders
    WHERE order_date >= DATE '2023-08-21'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Attach stores attributes
    SELECT a.*, b.sqft
    FROM base a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status, id
    FROM enriched
    INTERSECT
    SELECT status, id
    FROM orders
    WHERE discount <= 0.29
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.status = k.status AND s.id = k.id
),
enriched_orders AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_grp AS discount_cume_dist,
        DENSE_RANK() OVER w_recent AS discount_dense_rank,
        AVG(discount) OVER (PARTITION BY store_id) AS store_id_avg_discount
    FROM verified
    WINDOW w_grp AS (ORDER BY discount ASC NULLS LAST), w_recent AS (PARTITION BY store_id ORDER BY discount ASC NULLS LAST)
)
SELECT
    status,
    amount,
    discount,
    status_prefix,
    sqft,
    discount_dense_rank,
    store_id_avg_discount,
    CASE WHEN status RLIKE '^(PENDING|SHIPPED)$' THEN 'GROUP_A' WHEN status RLIKE '^(COMPLETED|CANCELLED)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS status_zone
FROM enriched_orders
ORDER BY store_id_avg_discount DESC NULLS FIRST
