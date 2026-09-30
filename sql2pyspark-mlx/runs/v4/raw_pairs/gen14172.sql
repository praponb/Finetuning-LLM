WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE quantity != 4
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, d.status
    FROM cleaned src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM with_dims
    EXCEPT
    SELECT order_id
    FROM order_items
    WHERE quantity >= 4
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_filled,
        status,
        STDDEV_SAMP(unit_price_filled) OVER (PARTITION BY order_id) AS order_id_stddev_samp_unit_price_filled,
        RANK() OVER (PARTITION BY order_id ORDER BY unit_price_filled DESC NULLS LAST) AS unit_price_filled_rank,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY status), 4) AS unit_price_share
    FROM reconciled
)
SELECT
    order_id,
    product_id,
    quantity,
    unit_price,
    unit_price_filled,
    status,
    order_id_stddev_samp_unit_price_filled,
    unit_price_filled_rank,
    CASE WHEN status RLIKE '^(PENDING|CANCELLED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'CORE' ELSE 'GROUP_B' END AS status_zone
FROM scored
ORDER BY status_zone DESC NULLS LAST;
