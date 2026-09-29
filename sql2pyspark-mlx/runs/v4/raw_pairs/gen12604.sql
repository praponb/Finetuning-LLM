WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(browser) AS clean_browser,
        CAST(started_at AS DATE) AS started_at_day
    FROM sessions
    WHERE country NOT IN ('SG', 'UK', 'US')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM filtered src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at < sub.end_date + INTERVAL 7 DAYS
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        clean_browser,
        started_at_day,
        subscription_id,
        plan,
        monthly_fee,
        RANK() OVER w_grp AS subscription_id_rank,
        LAST_VALUE(country) IGNORE NULLS OVER w_running AS last_known_country,
        ROW_NUMBER() OVER w_running_2 AS seq_no,
        ROUND(subscription_id / SUM(subscription_id) OVER (PARTITION BY customer_id, browser), 4) AS subscription_id_share
    FROM attributed
    WINDOW w_grp AS (PARTITION BY browser ORDER BY subscription_id DESC NULLS FIRST), w_running AS (PARTITION BY customer_id ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY country ORDER BY monthly_fee DESC, id, subscription_id)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM enriched a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at <= DATE_ADD(sub.end_date, 10)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(monthly_fee, 0) AS monthly_fee_rounded,
        ROUND(subscription_id_share * 1.07, 2) AS subscription_id_share_scaled
    FROM with_window
    WHERE country IN ('SG', 'UK')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY subscription_id_2 DESC NULLS FIRST, id, subscription_id, subscription_id_2) AS rn
        FROM base
    ) t
    WHERE rn = 1
)
SELECT
    id,
    device,
    started_at,
    country,
    clean_browser,
    started_at_day,
    subscription_id,
    monthly_fee,
    subscription_id_rank
FROM ranked
ORDER BY started_at DESC NULLS FIRST;
