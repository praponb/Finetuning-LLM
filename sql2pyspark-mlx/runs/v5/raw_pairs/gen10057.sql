WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        MONTH(opened_date) AS opened_date_month,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-09-23'
      AND status IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.is_flagged
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(opened_date_month) OVER w_running AS running_max_opened_date_month,
        SUM(opened_date_month) OVER w_running AS running_sum_opened_date_month,
        AVG(opened_date_month) OVER w_running AS running_avg_opened_date_month,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM eligible
    WHERE opened_date_month > 67
    WINDOW w_running AS (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY account_type, customer_id ORDER BY balance DESC, id)
),
eligible_accounts AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
enriched_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(event_seq) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_seq_avg_6,
        LEAD(event_seq, 1) OVER (PARTITION BY account_type ORDER BY running_avg_opened_date_month, id) AS next_event_seq
    FROM eligible_accounts
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        running_sum_opened_date_month
    FROM enriched_accounts
) src
PIVOT (
    SUM(running_sum_opened_date_month)
    FOR status IN ('OPEN', 'FROZEN', 'CLOSED')
)
