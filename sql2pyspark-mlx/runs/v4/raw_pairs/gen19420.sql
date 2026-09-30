WITH attributed AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date >= sub.start_date
       AND s.opened_date < sub.end_date + INTERVAL 14 DAYS
),
latest AS (
    SELECT *
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY monthly_fee DESC, id, subscription_id) = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id, subscription_id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
totals AS (
    SELECT
        CASE WHEN subscription_id >= 10 THEN 'LARGE' ELSE 'SMALL' END AS subscription_id_tier,
        COUNT_IF(plan = 'standard') AS standard_events,
        PERCENTILE_APPROX(subscription_id, 0.5) AS subscription_id_p50,
        SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
    FROM ranked
    GROUP BY CASE WHEN subscription_id >= 10 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    subscription_id_tier,
    standard_events,
    subscription_id_p50,
    account_type_list
FROM totals;
