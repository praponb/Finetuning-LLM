WITH valid AS (
    SELECT *, DATE_ADD(start_date, 30) AS start_date_plus_30d
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-12-11')
      AND customer_id IS NOT NULL
      AND MONTH(start_date) IN (2, 3, 12)
),
with_dims AS (
    SELECT s.*, d.segment
    FROM valid s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
enriched AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        start_date_plus_30d,
        segment,
        COUNT(monthly_fee) OVER (PARTITION BY status, customer_id) AS status_count_monthly_fee,
        RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC) AS monthly_fee_rank
    FROM with_dims
),
attributed AS (
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
metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        start_date_plus_30d,
        segment,
        status_count_monthly_fee,
        monthly_fee_rank,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        NTILE(10) OVER w_recent AS status_count_monthly_fee_bucket,
        FIRST_VALUE(status_count_monthly_fee) IGNORE NULLS OVER (ORDER BY monthly_fee_rank, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_count_monthly_fee,
        LAG(monthly_fee_rank, 2) OVER w_part AS prev_monthly_fee_rank,
        ROW_NUMBER() OVER (ORDER BY status_count_monthly_fee, id, subscription_id) AS seq_no
    FROM attributed
    WINDOW w_recent AS (PARTITION BY status ORDER BY status_count_monthly_fee, id, subscription_id), w_part AS (PARTITION BY customer_id ORDER BY start_date_plus_30d, id, subscription_id)
),
enriched_subscriptions AS (
    SELECT s.*, p.country
    FROM metrics s
    LEFT JOIN customers p
        ON s.customer_id = p.id
)
SELECT
    *,
    ROUND(prev_monthly_fee_rank / SUM(prev_monthly_fee_rank) OVER (PARTITION BY country), 4) AS prev_monthly_fee_rank_share,
    ROUND(status_count_monthly_fee / SUM(status_count_monthly_fee) OVER (PARTITION BY subscription_plan, plan), 4) AS status_count_monthly_fee_share,
    SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
FROM enriched_subscriptions
