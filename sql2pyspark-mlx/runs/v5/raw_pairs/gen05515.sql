WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        status,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        COALESCE(status, 'N/A') AS status_clean
    FROM accounts
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        opened_date_plus_months,
        status_clean,
        ROW_NUMBER() OVER (ORDER BY opened_date_plus_months, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date_plus_months DESC, id) AS event_seq_2
    FROM valid
    WHERE opened_date_plus_months > '2024-11-21'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(event_seq) IGNORE NULLS OVER (ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq,
        AVG(event_seq_2) OVER (PARTITION BY account_type ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_2_avg_3,
        SUM(event_seq) OVER w_hist AS cumulative_event_seq
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY account_type ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date_plus_months,
        s.event_seq,
        s.event_seq_2,
        s.first_known_event_seq,
        s.event_seq_2_avg_3,
        x.transaction_amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date_plus_months
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date_plus_months,
        event_seq,
        event_seq_2,
        AVG(event_seq_2_avg_3) OVER (ORDER BY transaction_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_seq_2_avg_3_avg_6,
        CUME_DIST() OVER (ORDER BY first_known_event_seq DESC NULLS LAST) AS first_known_event_seq_cume_dist,
        ROW_NUMBER() OVER (ORDER BY transaction_amount DESC, id) AS event_seq_3,
        NTILE(5) OVER (ORDER BY event_seq, id) AS event_seq_bucket
    FROM with_latest
)
-- Window metrics over several different partitions
SELECT
    *,
    LAST_VALUE(event_seq_2) IGNORE NULLS OVER (ORDER BY event_seq_2_avg_3_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq_2,
    CUME_DIST() OVER (ORDER BY event_seq_bucket DESC) AS event_seq_bucket_cume_dist,
    PERCENT_RANK() OVER (ORDER BY event_seq NULLS FIRST) AS event_seq_percent_rank
FROM enriched_accounts;
