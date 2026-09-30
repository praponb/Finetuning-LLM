WITH metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY started_at DESC, id) AS event_seq,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM sessions
    WINDOW w_grp AS (PARTITION BY device ORDER BY started_at, id)
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.country,
    s.seq_no,
    s.event_seq,
    s.row_seq,
    CASE WHEN device RLIKE '^(mobile|tablet)$' THEN 'OTHER_ZONE' WHEN device RLIKE '^(desktop)$' THEN 'CORE' ELSE 'GROUP_B' END AS device_zone,
    (SELECT SUM(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS sum_account_balance
FROM metrics s;
