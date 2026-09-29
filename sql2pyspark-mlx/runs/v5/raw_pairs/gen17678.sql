WITH enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.start_date,
        a.monthly_fee,
        a.status,
        d.email,
        d.segment,
        d.country
    FROM subscriptions a
    JOIN customers d
        ON a.customer_id = d.id
),
with_window AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM enriched a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date BETWEEN sub.start_date AND sub.end_date
),
base AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        email,
        segment,
        country,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        COALESCE(segment, 'Unknown') AS segment_filled
    FROM with_window
    WHERE start_date > DATE '2023-06-26'
      AND customer_id = 30
),
filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        segment,
        country,
        subscription_id,
        subscription_plan,
        INITCAP(TRIM(plan)) AS plan_title,
        UPPER(country) AS country_upper
    FROM base
    WHERE start_date >= DATE '2023-06-29'
      AND customer_id = 5
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY subscription_id NULLS FIRST) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
with_window_subscriptions AS (
    SELECT
        a.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee
    FROM latest a
    JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date BETWEEN sub.start_date AND sub.end_date
),
with_window_subscriptions_2 AS (
    SELECT
        s.*,
        sub.id AS subscription_id_3,
        sub.plan AS subscription_plan_3,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_window_subscriptions s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date BETWEEN sub.start_date AND sub.end_date
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    status,
    segment,
    subscription_id,
    subscription_plan,
    plan_title
FROM with_window_subscriptions_2
ORDER BY subscription_plan DESC;
