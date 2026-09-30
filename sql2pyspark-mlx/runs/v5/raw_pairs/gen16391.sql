WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date NULLS FIRST, id) AS dedup_rank
        FROM subscriptions
    ) x
    WHERE dedup_rank = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN plan IN ('standard', 'basic') THEN 'Group A' ELSE 'Other' END AS plan_group,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label
    FROM ranked
    WHERE end_date >= DATE '2023-07-24'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        dedup_rank,
        plan_group,
        end_date_label,
        LEAD(monthly_fee) OVER w_ordered AS next_monthly_fee,
        LAG(monthly_fee) OVER w_ordered AS prev_monthly_fee,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan, status), 4) AS monthly_fee_share
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY status ORDER BY monthly_fee, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        plan_group,
        row_seq,
        RANK() OVER (PARTITION BY status ORDER BY next_monthly_fee DESC NULLS LAST) AS next_monthly_fee_rank,
        ROW_NUMBER() OVER (ORDER BY row_seq, id) AS event_seq,
        FIRST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY status ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_seq
    FROM metrics
    WHERE monthly_fee_share IS NULL
)
-- Grouped metrics
SELECT
    CASE WHEN first_known_row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS first_known_row_seq_tier,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(status)) AS num_status
FROM enriched
GROUP BY first_known_row_seq_tier;
