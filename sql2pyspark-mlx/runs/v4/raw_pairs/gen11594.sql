WITH filtered AS (
    SELECT
        id,
        store_name,
        opened_date,
        DATE_SUB(opened_date, 34) AS opened_date_minus_34d
    FROM stores
    WHERE opened_date > DATE '2023-03-25'
      AND sqft > 12888
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY opened_date, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY opened_date_minus_34d, id) AS seq_no_2,
        ROW_NUMBER() OVER (ORDER BY opened_date_minus_34d DESC, id) AS row_seq
    FROM filtered
),
with_windows AS (
    SELECT
        id,
        store_name,
        opened_date,
        opened_date_minus_34d,
        seq_no,
        seq_no_2,
        row_seq,
        NTILE(4) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket,
        NTILE(3) OVER (ORDER BY seq_no_2 DESC, id) AS seq_no_2_bucket,
        AVG(seq_no_2) OVER (ORDER BY seq_no_2, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS seq_no_2_avg_5
    FROM enriched
),
enriched_stores AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY seq_no_2_avg_5, id) AS seq_no_2_avg_5_bucket,
        NTILE(4) OVER (ORDER BY seq_no_2_avg_5 DESC, id) AS seq_no_2_avg_5_bucket_2,
        LAST_VALUE(seq_no, TRUE) OVER (ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id) AS row_seq_2,
        LAST_VALUE(seq_no_2_bucket, TRUE) OVER w_recent AS last_known_seq_no_2_bucket
    FROM with_windows
    WINDOW w_recent AS (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    DATE_TRUNC('YEAR', opened_date_minus_34d) AS year_start,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(last_known_seq_no_2_bucket) AS min_last_known_seq_no_2_bucket,
    ROUND(AVG(seq_no_2), 2) AS avg_seq_no_2,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(last_known_seq_no_2_bucket, 0.95) AS last_known_seq_no_2_bucket_p95
FROM enriched_stores
GROUP BY 1
ORDER BY distinct_ids NULLS LAST
