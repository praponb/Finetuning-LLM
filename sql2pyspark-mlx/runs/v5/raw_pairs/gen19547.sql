WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        LENGTH(store_name) AS store_name_length,
        ROUND(sqft, 0) AS sqft_rounded,
        COALESCE(region, 'NONE') AS region_clean
    FROM stores
    WHERE opened_date > DATE '2023-03-04'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY store_name_length DESC NULLS FIRST, id) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.opened_date
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(store_name_length AS DOUBLE) AS store_name_length_num
    FROM eligible
    WHERE opened_date >= TO_DATE('2023-07-07')
      AND store_name LIKE 'A%'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        opened_date,
        store_name_length,
        sqft_rounded,
        region_clean,
        store_name_length_num
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft NULLS FIRST, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY store_name_length DESC NULLS LAST) AS store_name_length_cume_dist,
        AVG(store_name_length_num) OVER w_recent AS store_name_length_num_avg_5,
        LEAD(store_name_length, 1) OVER w_running AS next_store_name_length,
        PERCENT_RANK() OVER (ORDER BY sqft_rounded NULLS LAST) AS sqft_rounded_percent_rank,
        ROW_NUMBER() OVER w_recent_2 AS seq_no
    FROM ranked
    WINDOW w_recent AS (ORDER BY sqft_rounded, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_running AS (ORDER BY store_name_length_num, id), w_recent_2 AS (ORDER BY sqft_rounded, id)
)
-- Window metrics over several different partitions
SELECT
    id,
    sqft_rounded,
    store_name_length_num,
    store_name_length_num_avg_5,
    next_store_name_length,
    sqft_rounded_percent_rank,
    seq_no,
    AVG(sqft_rounded) OVER w_recent AS running_avg_sqft_rounded,
    MIN(sqft_rounded) OVER w_recent AS running_min_sqft_rounded,
    COUNT(sqft_rounded) OVER w_recent AS running_count_sqft_rounded,
    MAX(sqft_rounded) OVER w_recent AS running_max_sqft_rounded,
    RANK() OVER (ORDER BY next_store_name_length DESC NULLS LAST) AS next_store_name_length_rank
FROM enriched
WINDOW w_recent AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
