WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM stores
    WHERE opened_date > DATE '2023-04-23'
      AND region IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY days_since_opened_date DESC) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
latest_stores AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY row_num DESC, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        days_since_opened_date,
        row_num,
        rank_in_group
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rank_in_group DESC) AS dedup_rank
        FROM latest_stores
    ) t
    WHERE dedup_rank = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM deduped
    INTERSECT ALL
    SELECT id
    FROM stores
    WHERE store_name IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY days_since_opened_date NULLS LAST) AS days_since_opened_date_dense_rank,
        DENSE_RANK() OVER (ORDER BY days_since_opened_date NULLS FIRST) AS days_since_opened_date_dense_rank_2,
        DENSE_RANK() OVER w_part AS days_since_opened_date_dense_rank_3,
        PERCENT_RANK() OVER w_running AS days_since_opened_date_percent_rank,
        CUME_DIST() OVER w_running AS days_since_opened_date_cume_dist
    FROM reconciled
    WHERE row_num = 1
    WINDOW w_part AS (ORDER BY days_since_opened_date), w_running AS (ORDER BY days_since_opened_date DESC)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY days_since_opened_date_dense_rank_3 NULLS LAST) AS days_since_opened_date_dense_rank_3_dense_rank,
        RANK() OVER (ORDER BY days_since_opened_date NULLS FIRST) AS days_since_opened_date_rank
    FROM with_windows
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN days_since_opened_date_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS days_since_opened_date_rank_tier,
        APPROX_PERCENTILE(days_since_opened_date, ARRAY(0.25, 0.5, 0.75)) AS days_since_opened_date_quartiles,
        AVG(days_since_opened_date_dense_rank_3) AS avg_days_since_opened_date_dense_rank_3,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(days_since_opened_date_percent_rank) AS max_days_since_opened_date_percent_rank,
        PERCENTILE_APPROX(days_since_opened_date_dense_rank, 0.9) AS days_since_opened_date_dense_rank_p90
    FROM enriched
    GROUP BY CASE WHEN days_since_opened_date_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    days_since_opened_date_rank_tier,
    days_since_opened_date_quartiles,
    avg_days_since_opened_date_dense_rank_3,
    cnt,
    distinct_ids
FROM stats
ORDER BY distinct_ids DESC NULLS LAST, cnt
