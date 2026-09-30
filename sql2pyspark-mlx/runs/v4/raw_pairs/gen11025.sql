WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CAST(sqft AS STRING) AS sqft_num,
        ROUND(sqft, 0) AS sqft_rounded
    FROM stores
    WHERE opened_date > TO_DATE('2023-12-27')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_num,
        sqft_rounded,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft_rounded) AS sqft_rounded_cume_dist,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS seq_no
    FROM base
),
scored_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(seq_no) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY region), 4) AS seq_no_share,
        SUM(sqft) OVER (PARTITION BY city, region ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM scored
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(running_seq_no) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft_rounded_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_seq_no,
        AVG(seq_no_share) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seq_no_share_avg_6
    FROM scored_stores
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN sqft_rounded >= 89 THEN 'LARGE' ELSE 'SMALL' END AS sqft_rounded_tier,
        ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS seq_no_per_west,
        SUM(CASE WHEN region = 'North' THEN cumulative_sqft ELSE 0 END) AS north_cumulative_sqft,
        SUM(CASE WHEN region = 'East' THEN cumulative_sqft ELSE 0 END) AS east_cumulative_sqft,
        SUM(sqft_cume_dist) FILTER (WHERE city = 'Singapore') AS singapore_total_sqft_cume_dist,
        COUNT(*) AS cnt,
        GROUPING_ID() AS gid
    FROM with_windows
    GROUP BY CUBE (CASE WHEN sqft_rounded >= 89 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT seq_no_per_west, gid
FROM totals
ORDER BY seq_no_per_west DESC NULLS FIRST;
