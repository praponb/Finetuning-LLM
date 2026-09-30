WITH valid AS (
    SELECT *, CEIL(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
metrics AS (
    SELECT
        *,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee_whole, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC) AS monthly_fee_rank,
        LEAD(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS next_monthly_fee
    FROM valid
    WHERE status = 'CANCELLED'
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.end_date,
        a.monthly_fee_whole,
        a.monthly_fee_avg_6_prior,
        a.next_monthly_fee,
        c.age
    FROM metrics a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_avg_6_prior DESC NULLS LAST, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
valid_subscriptions AS (
    SELECT *, ROUND(latest_rank, 2) AS latest_rank_rounded
    FROM deduped
    WHERE monthly_fee_whole BETWEEN 60 AND 73
),
with_dims AS (
    SELECT a.*, p.city
    FROM valid_subscriptions a
    LEFT JOIN customers p
        ON a.customer_id = p.id
)
SELECT *
FROM (
    SELECT
        id,
        customer_id,
        next_monthly_fee,
        latest_rank_rounded
    FROM with_dims
) src
UNPIVOT (
    value FOR metric_name IN (next_monthly_fee, latest_rank_rounded)
);
