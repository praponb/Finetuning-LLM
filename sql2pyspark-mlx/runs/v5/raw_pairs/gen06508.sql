WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share_2
    FROM store_sales
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM scored
    EXCEPT
    SELECT product_id, id
    FROM store_sales
    WHERE product_id IN (4, 6, 8)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.opened_date,
        d.city,
        d.region
    FROM kept_rows src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
scored_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(revenue_share_2) OVER (PARTITION BY product_id) AS product_id_min_revenue_share_2,
        CUME_DIST() OVER (ORDER BY revenue_share_2 DESC NULLS LAST) AS revenue_share_2_cume_dist,
        ROUND(units / SUM(units) OVER (PARTITION BY product_id), 4) AS units_share
    FROM with_dims
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_share_2,
        opened_date,
        product_id_min_revenue_share_2,
        revenue_share_2_cume_dist,
        units_share,
        UPPER(city) AS city_upper,
        COALESCE(city, 'N/A') AS city_clean
    FROM scored_store_sales
    WHERE units_share < 0.41
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue_share_2,
    opened_date,
    revenue_share_2_cume_dist,
    units_share,
    city_upper
FROM filtered
ORDER BY product_id DESC NULLS LAST;
