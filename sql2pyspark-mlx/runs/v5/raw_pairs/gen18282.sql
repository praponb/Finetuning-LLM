WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, v
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM lines
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM islands
),
sessions_carts AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        created_at,
        k,
        v,
        session_id,
        SUM(is_new_session) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_carts
),
islands_carts AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps_2
),
gaps_3 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM islands_carts
),
sessions_carts_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_3
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag_2
    FROM sessions_carts_2
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps_2
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        k,
        v,
        session_start_flag,
        island_id,
        new_group_flag,
        SUM(new_group_flag) OVER w_hist running_new_group_flag,
        LAST_VALUE(session_start_flag_2) IGNORE NULLS OVER w_recent last_known_session_start_flag_2
    FROM sessionized
    WHERE session_no > 3
    WINDOW w_hist AS (PARTITION BY k ORDER BY burst_id ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY session_id ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
        SIZE(COLLECT_SET(k)) AS num_ks,
        MIN(new_group_flag) AS min_new_group_flag,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS new_group_flag_quartiles,
        COUNT(*) AS num_rows
    FROM enriched
    GROUP BY CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    session_start_flag_tier,
    num_ks,
    min_new_group_flag,
    cnt,
    new_group_flag_quartiles
FROM totals
ORDER BY num_ks DESC
