WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(session_start_flag, TRUE) OVER (PARTITION BY merchant, txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start_flag,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY session_start_flag, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        AVG(session_start_flag) OVER (ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS session_start_flag_avg_6,
        LEAD(session_id, 2) OVER (ORDER BY txn_time, id) AS next_session_id
    FROM sessions_transactions
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        txn_type,
        amount,
        is_flagged,
        session_start_flag,
        session_id,
        last_known_session_start_flag,
        amount_avg_3_prior,
        session_start_flag_avg_6,
        next_session_id
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY last_known_session_start_flag DESC NULLS LAST) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
scored_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(session_start_flag_avg_6) OVER (PARTITION BY txn_type ORDER BY session_id, id) AS prev_session_start_flag_avg_6,
        LEAD(session_start_flag_avg_6) OVER (PARTITION BY txn_type ORDER BY session_id, id) AS next_session_start_flag_avg_6,
        LAG(amount) OVER (PARTITION BY txn_type ORDER BY session_id, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY session_id, id) AS row_seq,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM ranked
),
ranked_transactions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount_avg_3_prior DESC, id) AS latest_rank
    FROM scored_transactions
    QUALIFY latest_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        txn_type,
        amount,
        is_flagged,
        session_start_flag,
        session_id,
        amount_avg_3_prior,
        session_start_flag_avg_6,
        next_session_id,
        prev_session_start_flag_avg_6,
        next_session_start_flag_avg_6,
        prev_amount,
        row_seq,
        first_known_amount,
        latest_rank,
        CASE WHEN prev_session_start_flag_avg_6 >= 1 THEN 'High' WHEN prev_session_start_flag_avg_6 >= 0 THEN 'Medium' ELSE 'Low' END AS prev_session_start_flag_avg_6_band
    FROM ranked_transactions
    WHERE amount BETWEEN 2500 AND 3900
)
-- Grouped metrics
SELECT
    txn_type,
    COUNT(*) FILTER (WHERE txn_type = 'FEE') AS fee_rows,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(amount_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS amount_avg_3_prior_quartiles
FROM cleaned
GROUP BY 1
ORDER BY txn_type
