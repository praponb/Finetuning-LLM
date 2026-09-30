WITH lines AS (
    SELECT
        id,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
cleaned AS (
    SELECT
        *,
        COALESCE(k, 'N/A') AS k_filled,
        UPPER(k) AS k_upper
    FROM lines
    WHERE status_code > 344
),
scored AS (
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY k), 4) AS status_code_share,
        LAST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY status_code ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k,
        CUME_DIST() OVER (PARTITION BY k ORDER BY status_code ASC) AS status_code_cume_dist
    FROM cleaned
),
scored_app_logs AS (
    SELECT
        *,
        SUM(status_code) OVER (PARTITION BY k) AS k_sum_status_code,
        SUM(status_code_cume_dist) OVER (PARTITION BY k ORDER BY status_code ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_cume_dist,
        NTILE(4) OVER (ORDER BY status_code ASC, id ASC, k ASC) AS status_code_bucket,
        FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY status_code_share ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        NTILE(10) OVER (PARTITION BY k ORDER BY status_code_cume_dist ASC, id ASC, k ASC) AS status_code_cume_dist_bucket
    FROM scored
    WHERE status_code > 280
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_bucket DESC, id ASC, k ASC) AS latest_rank
    FROM scored_app_logs
    QUALIFY latest_rank = 1
)
SELECT
    id,
    status_code,
    k,
    k_filled,
    status_code_share,
    last_known_k,
    status_code_cume_dist,
    k_sum_status_code,
    cumulative_status_code_cume_dist,
    status_code_bucket,
    latest_rank,
    AVG(status_code_cume_dist_bucket) OVER (PARTITION BY k ORDER BY latest_rank ASC, id ASC, k ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_code_cume_dist_bucket_avg_3_prior,
    SUM(cumulative_status_code_cume_dist) OVER (ORDER BY status_code_cume_dist ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_status_code_cume_dist
FROM deduped;
