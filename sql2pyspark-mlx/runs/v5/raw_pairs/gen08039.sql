WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE opened_date BETWEEN '2024-08-15' AND '2024-12-06'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT region, city
    FROM base
    INTERSECT ALL
    SELECT region, city
    FROM stores
    WHERE region IN ('West', 'Central')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.region = k.region AND s.city = k.city
),
scored AS (
    -- Ranking and gap features
    SELECT
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_month_end,
        region_clean,
        MIN(sqft) OVER (PARTITION BY city) AS city_min_sqft,
        RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST) AS sqft_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share
    FROM reconciled
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        sqft,
        sqft_rank,
        sqft_share,
        PERCENT_RANK() OVER (ORDER BY sqft_rank DESC NULLS FIRST) AS sqft_rank_percent_rank,
        ROUND(sqft_rank / SUM(sqft_rank) OVER (PARTITION BY region, city), 4) AS sqft_rank_share,
        RANK() OVER (PARTITION BY region, city ORDER BY city_min_sqft DESC) AS city_min_sqft_rank
    FROM scored
)
SELECT
    sqft_rank,
    sqft_rank_percent_rank,
    sqft_rank_share,
    city_min_sqft_rank
FROM with_windows
ORDER BY city_min_sqft_rank DESC NULLS LAST, sqft_rank
