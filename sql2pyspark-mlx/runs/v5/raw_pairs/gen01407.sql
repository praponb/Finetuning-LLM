WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(status) AS status_lower,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE paid_at > DATE '2023-05-27'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        status,
        status_lower,
        method_clean,
        is_new_session,
        island_id,
        NTILE(4) OVER (ORDER BY island_id ASC, id ASC) AS island_id_bucket,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method
    FROM sessionized
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status
    FROM metrics
    INTERSECT ALL
    SELECT status
    FROM payments
    WHERE status = 'FAILED'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.status = k.status
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        amount,
        status,
        status_lower,
        is_new_session,
        PERCENT_RANK() OVER (ORDER BY amount DESC) AS amount_percent_rank,
        DENSE_RANK() OVER (ORDER BY island_id ASC) AS island_id_dense_rank
    FROM verified
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        amount,
        status_lower,
        is_new_session,
        amount_percent_rank,
        island_id_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount_percent_rank ASC NULLS LAST) AS amount_percent_rank_percent_rank,
        RANK() OVER (ORDER BY is_new_session ASC NULLS FIRST) AS is_new_session_rank,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        STDDEV_SAMP(amount_percent_rank) OVER (PARTITION BY status) AS status_stddev_samp_amount_percent_rank,
        MAX(island_id_dense_rank) OVER (PARTITION BY status) AS status_max_island_id_dense_rank
    FROM enriched
    WHERE amount_percent_rank <= 0.53
),
enriched_payments AS (
    -- Running and moving aggregates
    SELECT
        status_lower,
        is_new_session,
        island_id_dense_rank,
        amount_share,
        status_stddev_samp_amount_percent_rank,
        status_max_island_id_dense_rank,
        CUME_DIST() OVER (ORDER BY amount_share DESC NULLS FIRST) AS amount_share_cume_dist,
        CUME_DIST() OVER (ORDER BY island_id_dense_rank ASC) AS island_id_dense_rank_cume_dist
    FROM scored
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN island_id_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS island_id_dense_rank_tier,
        CASE WHEN is_new_session >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_session_tier,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(island_id_dense_rank, 0.95) AS island_id_dense_rank_p95
    FROM enriched_payments
    GROUP BY CASE WHEN island_id_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN is_new_session >= 1 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    island_id_dense_rank_tier,
    is_new_session_tier,
    row_count,
    island_id_dense_rank_p95
FROM daily
