WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(quantity AS STRING) quantity_num
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity NULLS FIRST, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
latest_order_items AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS rank_in_group
        FROM latest
    ) ranked
    WHERE rank_in_group = 1
),
latest_order_items_2 AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest_order_items
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY latest_rank DESC) = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.cost,
        d.brand
    FROM latest_order_items_2 s
    LEFT JOIN products d
        ON s.product_id = d.id
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, d.launch_date
    FROM joined s
    LEFT JOIN products d
        ON s.product_id = d.id
    WHERE d.category = 'Grocery'
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost >= 770 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    AVG(unit_price) FILTER (WHERE brand = 'Stark') AS stark_avg_unit_price,
    APPROX_PERCENTILE(unit_price, 0.9) AS unit_price_p90,
    AVG(quantity) AS avg_quantity
FROM with_dims
GROUP BY CASE WHEN cost >= 770 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING AVG(unit_price) FILTER (WHERE brand = 'Stark') > 0
