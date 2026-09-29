WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) AS seq_no,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY customer_id, country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser
    FROM sessions
    WHERE browser IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY seq_no ASC, id ASC) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at <= DATE_ADD(sub.end_date, 3)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM attributed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    s.seq_no,
    s.rank_in_group,
    s.subscription_id,
    (SELECT AVG(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS avg_order_amount
FROM eligible s
