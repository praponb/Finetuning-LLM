WITH base AS (
    SELECT
        *,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
        FLOOR(sqft) AS sqft_whole
    FROM stores
    WHERE region IS NOT NULL
      AND region = 'East'
),
scored AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_label,
        sqft_whole,
        LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY sqft_whole, id) AS next_sqft,
        NTILE(5) OVER (PARTITION BY region, city ORDER BY sqft_whole, id) AS sqft_whole_bucket,
        AVG(sqft_whole) OVER (ORDER BY sqft_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_whole_avg_6
    FROM base
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY next_sqft DESC NULLS FIRST, id) AS rn
        FROM scored
    ) x
    WHERE rn = 1
),
deduped_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY next_sqft DESC NULLS LAST, id) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
with_windows AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_label,
        sqft_whole,
        next_sqft,
        sqft_whole_bucket,
        sqft_whole_avg_6,
        rn,
        dedup_rank,
        FIRST_VALUE(sqft_whole) IGNORE NULLS OVER (PARTITION BY city ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft_whole,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY next_sqft DESC, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY city ORDER BY next_sqft DESC NULLS LAST) AS next_sqft_cume_dist
    FROM deduped_stores
),
matched_keys AS (
    SELECT id, city
    FROM with_windows
    EXCEPT ALL
    SELECT id, city
    FROM stores
    WHERE MONTH(opened_date) IN (1, 3, 7)
),
kept_rows AS (
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.id = k.id AND s.city = k.city
),
filtered AS (
    SELECT
        *,
        ROUND(event_seq, 0) AS event_seq_rounded,
        YEAR(opened_date) AS opened_date_year,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM kept_rows
    WHERE opened_date > TO_DATE('2024-03-09')
),
summary AS (
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        city,
        COUNT(*) AS row_count,
        SUM(CASE WHEN region = 'East' THEN next_sqft ELSE 0 END) AS east_next_sqft,
        COUNT(*) AS cnt,
        MIN(sqft_whole) AS min_sqft_whole,
        COUNT_IF(region = 'North') AS north_events
    FROM filtered
    GROUP BY DATE_TRUNC('MONTH', opened_date), city
    HAVING min_sqft_whole > 0
)
SELECT
    activity_month,
    city,
    row_count,
    east_next_sqft,
    cnt,
    min_sqft_whole,
    north_events
FROM summary
ORDER BY cnt DESC
