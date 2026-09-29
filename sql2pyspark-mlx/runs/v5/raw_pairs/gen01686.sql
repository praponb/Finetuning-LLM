WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share,
        NTILE(5) OVER (ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY start_date, id) AS seq_no
    FROM subscriptions
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee NULLS FIRST, id) AS rn
        FROM scored
    ) t
    WHERE rn = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.name,
        p.signup_date,
        p.segment
    FROM ranked s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        monthly_fee_bucket,
        rn,
        name,
        AVG(monthly_fee_bucket) OVER (PARTITION BY status ORDER BY seq_no, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_bucket_avg_6,
        LEAD(monthly_fee_share, 2) OVER w_recent AS next_monthly_fee_share,
        SUM(monthly_fee) OVER w_hist AS cumulative_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY monthly_fee_share, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY end_date DESC, id) AS event_seq_2
    FROM joined
    WINDOW w_recent AS (PARTITION BY status, plan ORDER BY start_date, id), w_hist AS (PARTITION BY customer_id, plan ORDER BY monthly_fee_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        monthly_fee_bucket,
        name,
        cumulative_monthly_fee,
        event_seq,
        event_seq_2
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY event_seq_2 NULLS LAST, id) = 1
)
SELECT id, event_seq_2
FROM deduped;
