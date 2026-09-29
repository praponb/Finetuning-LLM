WITH matched AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM orders a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date >= sub.start_date
       AND a.order_date < sub.end_date + INTERVAL '14' DAY
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY discount DESC, id, subscription_id) AS rank_in_group
        FROM matched
    ) x
    WHERE rank_in_group = 1
),
kept AS (
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.order_id IS NULL
        )
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS row_seq,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM kept
    WINDOW w_running AS (ORDER BY discount, id, subscription_id), w_part AS (ORDER BY order_date DESC, id, subscription_id)
)
SELECT
    CASE WHEN subscription_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS subscription_id_tier,
    subscription_id,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS cnt,
    MIN(amount) AS min_amount,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list_2,
    ROUND(SUM(discount) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS discount_per_premium,
    GROUPING_ID() AS gid
FROM metrics
GROUP BY ROLLUP (CASE WHEN subscription_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, subscription_id)
HAVING ROUND(SUM(discount) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) < 1000000
ORDER BY subscription_id_tier NULLS LAST;
