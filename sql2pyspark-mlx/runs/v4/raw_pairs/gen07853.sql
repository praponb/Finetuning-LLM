WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 257 THEN 'High' WHEN quantity >= 103 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock >= DATE '2024-03-30'
),
joined AS (
    -- Attach stores attributes
    SELECT src.*, d.sqft
    FROM valid src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sqft DESC NULLS FIRST, id) AS row_num
        FROM joined
    ) t
    WHERE row_num = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity / row_num, 2) AS quantity_per_row_num
    FROM latest
    WHERE last_restock >= DATE '2023-03-28'
      AND quantity < 142
)
-- Ranking and gap features
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    quantity_tier,
    sqft,
    row_num,
    quantity_per_row_num,
    AVG(sqft) OVER (PARTITION BY product_id ORDER BY row_num, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
    AVG(quantity) OVER (ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_avg_6,
    STDDEV_SAMP(quantity) OVER (PARTITION BY store_id) AS store_id_stddev_samp_quantity
FROM filtered
