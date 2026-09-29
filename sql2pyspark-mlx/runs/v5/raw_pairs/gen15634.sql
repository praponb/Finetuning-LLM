WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        YEAR(end_date) AS end_date_year
    FROM subscriptions
    WHERE plan IN ('enterprise', 'standard', 'premium')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(end_date_year) OVER (PARTITION BY customer_id, status ORDER BY start_date, id) AS prev_end_date_year,
        LEAD(end_date_year) OVER (PARTITION BY customer_id, status ORDER BY start_date, id) AS next_end_date_year,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY start_date, id) AS row_seq
    FROM base
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        end_date,
        prev_end_date_year,
        row_seq,
        AVG(end_date_year) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS end_date_year_avg_2,
        PERCENT_RANK() OVER (ORDER BY end_date_year NULLS FIRST) AS end_date_year_percent_rank,
        LAST_VALUE(prev_end_date_year) IGNORE NULLS OVER (PARTITION BY plan ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_end_date_year
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        row_seq,
        end_date_year_avg_2,
        end_date_year_percent_rank,
        AVG(last_known_prev_end_date_year) OVER (PARTITION BY plan) AS plan_avg_last_known_prev_end_date_year,
        PERCENT_RANK() OVER w_grp AS end_date_year_percent_rank_percent_rank
    FROM enriched
    WINDOW w_grp AS (PARTITION BY plan ORDER BY end_date_year_percent_rank DESC NULLS FIRST)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN end_date_year_avg_2 >= 72 THEN 'HIGH' ELSE 'LOW' END AS end_date_year_avg_2_tier,
    CASE WHEN end_date_year_percent_rank >= 0.89 THEN 'LARGE' ELSE 'SMALL' END AS end_date_year_percent_rank_tier,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count,
    ROUND(AVG(plan_avg_last_known_prev_end_date_year), 2) AS avg_plan_avg_last_known_prev_end_date_year
FROM metrics
GROUP BY CASE WHEN end_date_year_avg_2 >= 72 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN end_date_year_percent_rank >= 0.89 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_plan_avg_last_known_prev_end_date_year > 0
ORDER BY avg_plan_avg_last_known_prev_end_date_year DESC NULLS FIRST
