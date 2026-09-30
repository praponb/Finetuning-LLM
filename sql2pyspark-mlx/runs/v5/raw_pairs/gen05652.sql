WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER w_running AS first_known_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit ASC, id ASC) AS row_seq
    FROM customers
    WHERE LOWER(name) LIKE '%ob %'
    WINDOW w_running AS (ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        credit_limit,
        first_known_credit_limit
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY row_seq DESC, id ASC) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(first_known_credit_limit) OVER w_running AS prev_first_known_credit_limit,
        LEAD(first_known_credit_limit) OVER w_running AS next_first_known_credit_limit,
        LEAD(credit_limit) OVER w_running AS next_credit_limit,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM ranked
    WINDOW w_running AS (ORDER BY first_known_credit_limit ASC, id ASC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        credit_limit,
        first_known_credit_limit,
        prev_first_known_credit_limit,
        next_first_known_credit_limit,
        ROW_NUMBER() OVER (ORDER BY prev_first_known_credit_limit DESC, id ASC) AS event_seq_2,
        AVG(next_credit_limit) OVER (ORDER BY credit_limit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS next_credit_limit_avg_3
    FROM enriched
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        credit_limit,
        first_known_credit_limit,
        prev_first_known_credit_limit,
        event_seq_2,
        next_credit_limit_avg_3,
        ROUND(prev_first_known_credit_limit, 1) AS prev_first_known_credit_limit_rounded,
        CEIL(next_credit_limit_avg_3) AS next_credit_limit_avg_3_whole
    FROM with_windows
    WHERE first_known_credit_limit BETWEEN 8600 AND 12300
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM customers
    WHERE segment = 'SMB'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id
),
scored_customers AS (
    -- Ranking and gap features
    SELECT
        id,
        credit_limit,
        prev_first_known_credit_limit,
        event_seq_2,
        next_credit_limit_avg_3_whole,
        CUME_DIST() OVER (ORDER BY credit_limit ASC NULLS LAST) AS credit_limit_cume_dist,
        PERCENT_RANK() OVER (ORDER BY prev_first_known_credit_limit DESC NULLS FIRST) AS prev_first_known_credit_limit_percent_rank,
        DENSE_RANK() OVER (ORDER BY prev_first_known_credit_limit_rounded DESC NULLS LAST) AS prev_first_known_credit_limit_rounded_dense_rank,
        CUME_DIST() OVER (ORDER BY next_credit_limit_avg_3 ASC) AS next_credit_limit_avg_3_cume_dist
    FROM reconciled
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_first_known_credit_limit_rounded_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS prev_first_known_credit_limit_rounded_dense_rank_tier,
        CASE WHEN event_seq_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS event_seq_2_tier,
        AVG(next_credit_limit_avg_3_cume_dist) AS avg_next_credit_limit_avg_3_cume_dist,
        APPROX_PERCENTILE(prev_first_known_credit_limit_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS prev_first_known_credit_limit_percent_rank_quartiles
    FROM scored_customers
    GROUP BY GROUPING SETS ((CASE WHEN prev_first_known_credit_limit_rounded_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN event_seq_2 >= 7 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN prev_first_known_credit_limit_rounded_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END))
)
SELECT
    prev_first_known_credit_limit_rounded_dense_rank_tier,
    event_seq_2_tier,
    avg_next_credit_limit_avg_3_cume_dist,
    prev_first_known_credit_limit_percent_rank_quartiles
FROM totals;
