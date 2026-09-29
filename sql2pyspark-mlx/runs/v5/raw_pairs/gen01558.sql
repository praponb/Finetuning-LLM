WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY log_ts, id, k) AS dedup_rank
    FROM lines
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_recent AS status_code_rank,
        MAX(status_code) OVER w_hist AS status_code_max_15m
    FROM deduped
    WINDOW w_recent AS (PARTITION BY level ORDER BY status_code), w_hist AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(status_code_rank) OVER (ORDER BY log_ts, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_rank_avg_6_prior,
        SUM(status_code) OVER w_hist AS running_status_code,
        COUNT(status_code_max_15m) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_15m_count_30m
    FROM with_windows
    WHERE level = 'DEBUG'
    WINDOW w_hist AS (PARTITION BY host, k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        level,
        k,
        value,
        dedup_rank,
        status_code_max_15m,
        running_status_code,
        DENSE_RANK() OVER (PARTITION BY k, host ORDER BY running_status_code DESC NULLS FIRST) AS running_status_code_dense_rank,
        FIRST_VALUE(status_code_rank, TRUE) OVER (PARTITION BY host, level ORDER BY status_code_rank_avg_6_prior, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_code_rank
    FROM metrics
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY first_known_status_code_rank DESC, id, k) AS dedup_rank_2
        FROM scored
    ) t
    WHERE dedup_rank_2 = 1
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            level,
            k,
            host,
            first_known_status_code_rank
        FROM latest
    ) src
    PIVOT (
        SUM(first_known_status_code_rank) AS amount
        FOR host IN ('h3', 'h4', 'h2')
    )
)
SELECT
    level,
    k,
    h3,
    h2
FROM by_type
ORDER BY h2 DESC NULLS FIRST;
