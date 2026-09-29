WITH enriched AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.amount,
        e.merchant,
        e.is_flagged,
        d.status
    FROM transactions e
    INNER JOIN accounts d
        ON e.account_id = d.id
    WHERE d.status IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        is_flagged,
        status,
        NTILE(5) OVER w_hist AS amount_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status, account_id), 4) AS amount_share
    FROM enriched
    WINDOW w_hist AS (ORDER BY amount, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        status,
        amount_bucket,
        amount_share,
        SUM(amount_bucket) OVER (PARTITION BY account_id ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_id, status ORDER BY amount_bucket, id) AS event_seq,
        PERCENT_RANK() OVER w_running AS amount_share_percent_rank
    FROM metrics
    WINDOW w_running AS (PARTITION BY status, account_id ORDER BY amount_share DESC NULLS LAST)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY event_seq DESC) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT account_id
    FROM deduped
    INTERSECT ALL
    SELECT account_id
    FROM transactions
    WHERE merchant IN ('Amazon', '7-Eleven', 'Lazada')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.account_id = k.account_id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.amount_share,
        src.running_amount_bucket,
        src.amount_share_percent_rank,
        b.opened_date
    FROM kept_rows src
    LEFT JOIN accounts b
        ON src.account_id = b.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        ROUND(AVG(amount_share_percent_rank), 2) AS avg_amount_share_percent_rank,
        COUNT(DISTINCT account_id) AS distinct_account_ids,
        ROUND(AVG(amount_share_percent_rank), 2) AS avg_amount_share_percent_rank_2,
        COUNT(*) AS row_count,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date) ORDER BY ROUND(AVG(amount_share_percent_rank), 2) DESC) AS avg_amount_share_percent_rank_dense_rank
    FROM with_dims
    GROUP BY DATE_TRUNC('WEEK', opened_date), DATE_TRUNC('MONTH', opened_date)
)
SELECT
    week_start,
    activity_month,
    avg_amount_share_percent_rank,
    row_count,
    avg_amount_share_percent_rank_dense_rank
FROM totals
ORDER BY week_start DESC;
