WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessions a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at <= DATE_ADD(sub.end_date, 10)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        country,
        subscription_id,
        monthly_fee,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY subscription_id NULLS FIRST) AS subscription_id_dense_rank,
        AVG(monthly_fee) OVER (ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
        SUM(subscription_id) OVER w_ordered AS subscription_id_sum_7d
    FROM matched
    WHERE country IN ('JP', 'DE', 'US')
    WINDOW w_ordered AS (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id = 6
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY device), 4) AS monthly_fee_share,
        AVG(monthly_fee) OVER (ORDER BY subscription_id_sum_7d, id, subscription_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior
    FROM kept
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.web_event_event_time,
        top1.web_event_page
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(subscription_id_sum_7d) OVER (PARTITION BY web_event_page, device ORDER BY monthly_fee_avg_2_prior, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_subscription_id_sum_7d,
        SUM(subscription_id_sum_7d) OVER (PARTITION BY country ORDER BY subscription_id_sum_7d, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_subscription_id_sum_7d,
        AVG(subscription_id) OVER (PARTITION BY web_event_page ORDER BY subscription_id_sum_7d, id, subscription_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_6_prior,
        SUM(subscription_id_dense_rank) OVER (PARTITION BY device ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_subscription_id_dense_rank,
        SUM(subscription_id_sum_7d) OVER (PARTITION BY country ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_subscription_id_sum_7d_2
    FROM with_last
),
with_windows_sessions_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        subscription_id,
        monthly_fee_avg_5,
        monthly_fee_share,
        monthly_fee_avg_2_prior,
        web_event_event_time,
        running_subscription_id_sum_7d,
        subscription_id_avg_6_prior,
        PERCENT_RANK() OVER (PARTITION BY device, country ORDER BY monthly_fee_share) AS monthly_fee_share_percent_rank,
        SUM(subscription_id_avg_6_prior) OVER (PARTITION BY device ORDER BY subscription_id_sum_7d, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_subscription_id_avg_6_prior
    FROM with_windows_sessions
)
SELECT
    id,
    subscription_id,
    monthly_fee_avg_5,
    monthly_fee_share,
    monthly_fee_avg_2_prior,
    web_event_event_time,
    subscription_id_avg_6_prior,
    monthly_fee_share_percent_rank,
    running_subscription_id_avg_6_prior
FROM with_windows_sessions_2
ORDER BY web_event_event_time;
