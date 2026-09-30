WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        LENGTH(store_name) store_name_length,
        COALESCE(region, 'OTHER') region_filled
    FROM stores
    WHERE region IS NOT DISTINCT FROM 'Central'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY store_name_length DESC NULLS FIRST, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.store_name_length,
        s.rank_in_group,
        latest.store_sale_units
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        store_name_length,
        store_sale_units,
        CEIL(store_sale_units) store_sale_units_whole
    FROM latest_match
    WHERE store_sale_units <= 18
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN store_sale_units >= 43 THEN 'HIGH' ELSE 'LOW' END AS store_sale_units_tier,
        CASE WHEN store_sale_units_whole >= 80 THEN 'LARGE' ELSE 'SMALL' END AS store_sale_units_whole_tier,
        COUNT(*) AS row_count,
        COUNT(*) AS cnt,
        APPROX_PERCENTILE(store_sale_units, 0.5) AS store_sale_units_p50
    FROM base
    GROUP BY CASE WHEN store_sale_units >= 43 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN store_sale_units_whole >= 80 THEN 'LARGE' ELSE 'SMALL' END
    HAVING COUNT(*) > 1
)
SELECT
    store_sale_units_tier,
    store_sale_units_whole_tier,
    cnt,
    store_sale_units_p50
FROM grouped
ORDER BY store_sale_units_whole_tier NULLS FIRST;
