WITH joined AS (
    SELECT
        e.*,
        d.country,
        d.name,
        d.credit_limit
    FROM subscriptions e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY credit_limit DESC, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
cleaned AS (
    SELECT *, FLOOR(credit_limit) AS credit_limit_whole
    FROM latest
    WHERE end_date >= DATE '2023-05-19'
      AND country IS NOT NULL
      AND MONTH(end_date) IN (8, 10, 11)
),
with_windows AS (
    SELECT
        *,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY credit_limit_whole, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_3_prior
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY plan ORDER BY start_date, id)
),
remaining_keys AS (
    SELECT status, plan
    FROM with_windows
    EXCEPT
    SELECT status, plan
    FROM subscriptions
    WHERE customer_id = 17
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.plan = k.plan
)
SELECT
    status,
    ROUND(AVG(cumulative_monthly_fee), 2) AS avg_cumulative_monthly_fee,
    ROUND(SUM(monthly_fee_share) / NULLIF(COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END), 0), 2) AS monthly_fee_share_per_expired
FROM reconciled
GROUP BY 1
ORDER BY status DESC NULLS FIRST;
