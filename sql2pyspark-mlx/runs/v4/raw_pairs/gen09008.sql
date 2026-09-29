WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(units / revenue, 2) revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-04-12'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(units, TRUE) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_units,
        STDDEV_SAMP(units) OVER (PARTITION BY store_id, product_id) store_id_stddev_samp_units,
        LAG(units, 1) OVER (PARTITION BY store_id ORDER BY revenue, id) prev_units
    FROM valid
),
kept_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM scored
    INTERSECT
    SELECT store_id, id
    FROM store_sales
    WHERE revenue > 3200
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(revenue_per_unit AS INT) revenue_per_unit_int
    FROM verified
    WHERE sale_date >= DATE '2023-06-11'
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.price,
        b.cost,
        b.launch_date
    FROM cleaned src
    LEFT JOIN products b
        ON src.product_id = b.id
),
valid_store_sales AS (
    -- Basic cleansing and defaults
    SELECT
        store_id,
        product_id,
        units,
        revenue,
        revenue_per_unit,
        store_id_stddev_samp_units,
        prev_units,
        revenue_per_unit_int,
        cost,
        ROUND(units, 0) units_rounded
    FROM enriched
    WHERE price >= 860
)
SELECT
    product_id,
    revenue,
    store_id_stddev_samp_units,
    revenue_per_unit_int,
    units_rounded
FROM valid_store_sales
ORDER BY units_rounded
