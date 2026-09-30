WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(region, 1, 2) AS region_prefix
    FROM stores
    WHERE region = 'South'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn
    FROM filtered
    QUALIFY rn = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date >= s.opened_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        region_prefix,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        NTILE(4) OVER (ORDER BY sqft DESC, id) AS sqft_bucket
    FROM not_flagged
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date NULLS LAST) AS rank_in_group
        FROM scored
    ) t
    WHERE rank_in_group = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_share DESC, id) AS rn
    FROM latest
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    APPROX_PERCENTILE(sqft_share, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_quartiles,
    BIT_OR(sqft) AS sqft_bit_or,
    ROUND(AVG(sqft_share), 2) AS avg_sqft_share,
    PERCENTILE_APPROX(first_known_sqft, ARRAY(0.25, 0.5, 0.75)) AS first_known_sqft_quartiles,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    COUNT(DISTINCT id) AS distinct_ids
FROM ranked
GROUP BY DATE_TRUNC('WEEK', opened_date), DATE_TRUNC('YEAR', opened_date)
HAVING distinct_ids >= 1
ORDER BY year_start DESC NULLS LAST
