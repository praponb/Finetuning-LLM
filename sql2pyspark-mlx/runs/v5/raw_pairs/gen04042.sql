WITH filtered AS (
    SELECT
        id,
        store_id,
        product_id,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE quantity BETWEEN 190 AND 230
),
with_dims AS (
    SELECT
        s.*,
        p2.sqft,
        p2.region
    FROM filtered s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
ranked AS (
    SELECT
        id,
        product_id,
        last_restock_month_end,
        sqft,
        region
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY sqft DESC NULLS LAST) AS rank_in_group
        FROM with_dims
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket_2,
        LAG(sqft, 1) OVER (PARTITION BY product_id ORDER BY last_restock_month_end, id) AS prev_sqft
    FROM ranked
)
SELECT
    id,
    sqft,
    region,
    sqft_bucket_2,
    prev_sqft
FROM enriched
ORDER BY sqft DESC
