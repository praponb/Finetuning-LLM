WITH valid AS (
    SELECT
        *,
        DATE_TRUNC('year', opened_date) AS opened_date_period,
        COALESCE(region, 'N/A') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
      AND store_name LIKE 'Mal%'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date DESC, id) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    SELECT
        id,
        store_name,
        opened_date_period,
        rank_in_group,
        NTILE(5) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket,
        ROW_NUMBER() OVER w_part AS event_seq,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS seq_no,
        PERCENT_RANK() OVER w_hist AS sqft_percent_rank,
        NTILE(3) OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS sqft_bucket_2
    FROM deduped
    WINDOW w_part AS (PARTITION BY region ORDER BY opened_date DESC, id), w_hist AS (PARTITION BY city ORDER BY sqft DESC NULLS FIRST)
),
scored AS (
    SELECT
        id,
        store_name,
        opened_date_period,
        sqft_bucket,
        ROW_NUMBER() OVER (ORDER BY sqft_bucket_2, id) AS seq_no_2,
        LAST_VALUE(seq_no, TRUE) OVER (ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        LAG(sqft_bucket, 1) OVER (ORDER BY event_seq, id) AS prev_sqft_bucket
    FROM with_windows
)
SELECT
    CASE WHEN seq_no_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_2_tier,
    (UNIX_TIMESTAMP(MAX(opened_date_period)) - UNIX_TIMESTAMP(MIN(opened_date_period))) / 60 AS span_minutes,
    PERCENTILE_APPROX(seq_no_2, 0.5) AS seq_no_2_p50,
    MIN(opened_date_period) AS started_at
FROM scored
GROUP BY CASE WHEN seq_no_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY seq_no_2_tier NULLS LAST
