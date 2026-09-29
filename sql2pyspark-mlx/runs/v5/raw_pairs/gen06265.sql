WITH with_last AS (
    SELECT s.*, top1.subscription_status
    FROM customers s
    JOIN LATERAL (
            SELECT c.status AS subscription_status
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_customers AS (
    SELECT
        s.*,
        x.session_browser,
        x.session_device
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.browser AS session_browser, c.device AS session_device
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.name,
        s.segment,
        s.is_active,
        s.session_browser,
        s.session_device,
        latest.browser
    FROM with_last_customers s
    LEFT JOIN LATERAL (
            SELECT c.browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_customers_2 AS (
    SELECT
        s.id,
        s.is_active,
        s.browser,
        top1.session_browser_2
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.browser AS session_browser_2
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    browser,
    session_browser_2,
    CASE WHEN session_browser_2 RLIKE '^(Safari|Firefox)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS session_browser_2_zone
FROM with_last_customers_2
ORDER BY session_browser_2_zone ASC NULLS FIRST, browser DESC;
