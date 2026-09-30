WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        LAST_DAY(end_date) AS end_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_monthly_fee,
        COUNT(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY end_date_month_end, id) AS seq_no
    FROM filtered
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.age,
        p.name
    FROM scored s
    LEFT JOIN customers p
        ON s.customer_id = p.id
    WHERE NOT p.is_active
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        end_date,
        end_date_month_end,
        running_max_monthly_fee,
        running_count_monthly_fee,
        running_avg_monthly_fee
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date_month_end DESC NULLS LAST, id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        end_date,
        end_date_month_end,
        running_max_monthly_fee,
        running_count_monthly_fee,
        running_avg_monthly_fee,
        ROUND(running_avg_monthly_fee, 0) AS running_avg_monthly_fee_rounded
    FROM latest
    WHERE end_date <= '2023-09-12'
)
-- Ranking and gap features
SELECT
    id,
    end_date_month_end,
    running_max_monthly_fee,
    running_count_monthly_fee,
    running_avg_monthly_fee,
    running_avg_monthly_fee_rounded,
    SUM(running_count_monthly_fee) OVER (ORDER BY running_max_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count_monthly_fee,
    NTILE(4) OVER (ORDER BY running_avg_monthly_fee_rounded DESC, id) AS running_avg_monthly_fee_rounded_bucket
FROM cleaned
