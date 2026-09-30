WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(city, ' ', region) AS city_region
    FROM stores
    WHERE opened_date > TO_DATE('2023-07-03')
      AND region IS NOT NULL
      AND sqft >= 5432
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        city_region,
        rank_in_group,
        PERCENT_RANK() OVER (PARTITION BY city, region ORDER BY sqft DESC) AS sqft_percent_rank,
        NTILE(10) OVER (ORDER BY sqft DESC, id) AS sqft_bucket,
        LEAD(sqft, 1, 0) OVER (PARTITION BY region, city ORDER BY rank_in_group, id) AS next_sqft
    FROM deduped
),
filtered_stores AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        sqft,
        city_region,
        rank_in_group,
        sqft_bucket,
        next_sqft,
        sqft_percent_rank + next_sqft AS sqft_percent_rank_plus_next_sqft,
        next_sqft - rank_in_group AS next_sqft_minus_rank_in_group,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM scored
    WHERE opened_date >= DATE '2023-10-16'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft_bucket NULLS LAST, id) AS latest_rank
        FROM filtered_stores
    ) ranked
    WHERE latest_rank = 1
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.revenue
    FROM latest s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped_stores AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY latest_rank DESC, id) AS latest_rank_2
        FROM latest_match
    ) x
    WHERE latest_rank_2 = 1
)
SELECT
    id,
    city,
    sqft,
    city_region,
    rank_in_group,
    sqft_bucket,
    next_sqft,
    sqft_percent_rank_plus_next_sqft,
    next_sqft_minus_rank_in_group,
    CASE WHEN city RLIKE '^(Chiang Mai|Tokyo)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'PRIORITY' ELSE 'CORE' END AS city_zone
FROM deduped_stores
ORDER BY city_zone DESC NULLS LAST, id DESC
LIMIT 10
