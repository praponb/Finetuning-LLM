WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(browser, TRUE) OVER w_grp AS last_known_browser,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS row_seq,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY browser, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM sessions
    WINDOW w_grp AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    s.last_known_browser,
    s.row_seq,
    CASE WHEN device RLIKE '^(desktop|mobile)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS device_zone,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM metrics s
ORDER BY country DESC NULLS LAST
