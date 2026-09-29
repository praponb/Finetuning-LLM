WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(method) clean_method,
        COALESCE(method, 'N/A') method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-07-08 20:00:00'
      AND order_id IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        method_clean,
        is_new_session,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY paid_at DESC, id) seq_no,
        LEAD(session_id, 1, 0) OVER (PARTITION BY method, order_id ORDER BY is_new_session, id) next_session_id,
        AVG(session_id) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) session_id_avg_3_prior,
        SUM(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_sum_30d
    FROM sessions_payments
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        paid_at,
        is_new_session,
        session_id,
        seq_no,
        AVG(next_session_id) OVER (PARTITION BY order_id ORDER BY amount_sum_30d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) next_session_id_avg_6,
        PERCENT_RANK() OVER (ORDER BY is_new_session NULLS LAST) is_new_session_percent_rank,
        LAG(is_new_session, 1) OVER (PARTITION BY order_id ORDER BY next_session_id, id) prev_is_new_session
    FROM enriched
    WHERE session_id_avg_3_prior < 5
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        paid_at,
        is_new_session,
        next_session_id_avg_6,
        prev_is_new_session,
        SUM(is_new_session) OVER (ORDER BY is_new_session_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_is_new_session,
        SUM(prev_is_new_session) OVER (ORDER BY next_session_id_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_prev_is_new_session,
        AVG(is_new_session) OVER (ORDER BY is_new_session, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) is_new_session_avg_5,
        RANK() OVER (ORDER BY session_id DESC) session_id_rank,
        MAX(prev_is_new_session) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) prev_is_new_session_max_7d
    FROM enriched_payments
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', paid_at) AS year_start,
        DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
        MIN(paid_at) AS first_paid_at,
        APPROX_PERCENTILE(is_new_session_avg_5, 0.9) AS is_new_session_avg_5_p90,
        SUM(next_session_id_avg_6) AS total_next_session_id_avg_6,
        MIN(paid_at) AS first_paid_at_2,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING(DATE_TRUNC('YEAR', paid_at)) AS grouping_id
    FROM with_windows
    GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', paid_at), DATE_TRUNC('QUARTER', paid_at)), (DATE_TRUNC('YEAR', paid_at)), ())
)
SELECT
    year_start,
    quarter_start,
    first_paid_at,
    is_new_session_avg_5_p90,
    first_paid_at_2,
    distinct_ids,
    grouping_id
FROM daily
ORDER BY is_new_session_avg_5_p90 DESC NULLS LAST;
