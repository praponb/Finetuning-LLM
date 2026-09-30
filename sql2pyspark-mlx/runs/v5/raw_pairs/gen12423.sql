WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END is_active_label,
        CONCAT(name, '/', city) name_city,
        COALESCE(segment, 'UNKNOWN') segment_clean
    FROM customers
    WHERE signup_date >= DATE '2023-05-13'
      AND segment IS NOT NULL
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.segment,
        s.signup_date,
        s.is_active,
        s.is_active_label,
        top1.subscription_plan,
        top1.subscription_end_date
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.plan AS subscription_plan, c.end_date AS subscription_end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, subscription_plan ORDER BY subscription_end_date, id) AS latest_rank
    FROM with_last
    QUALIFY latest_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at < '2025-06-22'
        )
)
SELECT
    id,
    name,
    email,
    is_active,
    is_active_label,
    subscription_plan,
    subscription_end_date
FROM eligible
ORDER BY id NULLS FIRST;
