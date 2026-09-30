WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE customer_id IN (38, 39)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at_date DESC, id) AS row_seq,
        ROW_NUMBER() OVER w_hist AS row_seq_2,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM filtered
    WINDOW w_hist AS (PARTITION BY browser, country ORDER BY started_at_date DESC, id), w_recent AS (PARTITION BY browser ORDER BY started_at_date, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at_date
        )
),
with_windows_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        started_at_label,
        started_at_date,
        row_seq,
        row_seq_2,
        event_seq,
        SUM(event_seq) OVER (ORDER BY started_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY device), 4) AS event_seq_share,
        LAG(row_seq_2, 1) OVER (PARTITION BY customer_id, device ORDER BY started_at_date, id) AS prev_row_seq_2,
        NTILE(4) OVER (ORDER BY row_seq_2, id) AS row_seq_2_bucket
    FROM kept
)
SELECT
    s.id,
    s.customer_id,
    s.country,
    s.started_at_label,
    s.row_seq_2,
    s.event_seq,
    s.running_event_seq,
    s.event_seq_share,
    s.prev_row_seq_2,
    CASE WHEN country RLIKE '^(UK|SG)$' THEN 'GROUP_B' WHEN country RLIKE '^(TH|US)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS country_zone,
    (SELECT MIN(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM with_windows_sessions s
ORDER BY row_seq_2, running_event_seq;
