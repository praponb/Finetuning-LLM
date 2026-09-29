WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at DESC, id, k) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at, id, k) AS event_seq_2,
        FIRST_VALUE(k, TRUE) OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at DESC, id, k) AS event_seq_3,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at, id, k) AS row_seq
    FROM lines
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY event_seq_3 DESC NULLS FIRST, id, k) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM ranked
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 10 AND 18
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN kept_keys k ON s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        k,
        value,
        event_seq,
        event_seq_2,
        first_known_k,
        event_seq_3,
        row_seq,
        latest_rank,
        RANK() OVER (PARTITION BY k ORDER BY event_seq_3 DESC NULLS LAST) AS event_seq_3_rank,
        DENSE_RANK() OVER (ORDER BY event_seq_2 NULLS FIRST) AS event_seq_2_dense_rank,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY event_seq NULLS LAST) AS event_seq_dense_rank,
        ROUND(event_seq_2 / SUM(event_seq_2) OVER (PARTITION BY k), 4) AS event_seq_2_share
    FROM verified
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        k,
        value,
        event_seq,
        event_seq_2,
        first_known_k,
        event_seq_3,
        row_seq,
        latest_rank,
        event_seq_3_rank,
        event_seq_2_dense_rank,
        event_seq_dense_rank,
        event_seq_2_share,
        ROUND(event_seq_dense_rank / SUM(event_seq_dense_rank) OVER (PARTITION BY k), 4) AS event_seq_dense_rank_share,
        RANK() OVER (ORDER BY event_seq_2_dense_rank NULLS FIRST) AS event_seq_2_dense_rank_rank,
        MIN(event_seq_2_dense_rank) OVER (PARTITION BY k) AS k_min_event_seq_2_dense_rank
    FROM scored
)
-- Ranking and gap features
SELECT
    *,
    SUM(event_seq_3_rank) OVER (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS event_seq_3_rank_sum_2h,
    COUNT(event_seq_2_dense_rank) OVER (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS event_seq_2_dense_rank_count_1h,
    PERCENT_RANK() OVER (ORDER BY event_seq_2 NULLS LAST) AS event_seq_2_percent_rank
FROM metrics;
