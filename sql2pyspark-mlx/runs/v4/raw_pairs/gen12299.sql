WITH base AS (
    SELECT *, LOWER(region) AS region_lower
    FROM stores
    WHERE opened_date > DATE '2023-08-19'
      AND region IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city, region ORDER BY opened_date) AS rn
        FROM base
    ) x
    WHERE rn = 1
),
cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        rn,
        ROUND(rn / sqft, 2) AS rn_per_sqft,
        FLOOR(rn) AS rn_whole
    FROM ranked
    WHERE opened_date > DATE '2023-09-20'
),
metrics AS (
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY city) AS city_sum_sqft,
        SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        LAST_VALUE(sqft) IGNORE NULLS OVER (ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft
    FROM cleaned
)
SELECT
    id,
    store_name,
    city,
    region,
    rn,
    rn_per_sqft,
    rn_whole,
    city_sum_sqft,
    cumulative_sqft
FROM metrics
ORDER BY region DESC
