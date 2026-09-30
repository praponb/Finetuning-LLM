WITH valid AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        LENGTH(browser) AS browser_length,
        COALESCE(country, 'UNKNOWN') AS country_filled
    FROM sessions
    WHERE browser IN ('Firefox', 'Edge')
),
with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM valid src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at BETWEEN sub.start_date AND sub.end_date
),
enriched AS (
    SELECT
        id,
        device,
        started_at,
        country,
        browser_length,
        subscription_id,
        monthly_fee,
        SUM(browser_length) OVER (PARTITION BY device ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_browser_length,
        LEAD(subscription_id, 1) OVER (PARTITION BY country ORDER BY started_at, id, subscription_id) AS next_subscription_id
    FROM with_window
    WHERE country = 'TH'
),
flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id, subscription_id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessionized AS (
    SELECT
        id,
        started_at,
        country,
        subscription_id,
        monthly_fee,
        running_browser_length,
        SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    SELECT
        id,
        subscription_id,
        monthly_fee,
        LEAD(running_browser_length, 1, 0) OVER w_recent AS next_running_browser_length,
        AVG(burst_id) OVER (ORDER BY started_at, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS burst_id_avg_6,
        LAG(running_browser_length, 1) OVER (PARTITION BY country ORDER BY started_at, id, subscription_id) AS prev_running_browser_length
    FROM sessionized
    WINDOW w_recent AS (PARTITION BY country ORDER BY subscription_id, id, subscription_id)
)
SELECT
    id,
    subscription_id,
    burst_id_avg_6,
    prev_running_browser_length
FROM metrics
