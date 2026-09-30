WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        start_date,
        end_date,
        LEAD(monthly_fee) OVER w_recent AS next_monthly_fee,
        LAG(monthly_fee) OVER w_recent AS prev_monthly_fee,
        ROW_NUMBER() OVER w_recent AS row_seq,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER w_grp AS last_known_monthly_fee
    FROM subscriptions
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY start_date, id), w_grp AS (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        YEAR(end_date) AS end_date_year,
        CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END AS is_enterprise
    FROM enriched
    WHERE end_date >= DATE '2023-07-02'
      AND plan = 'enterprise'
),
filtered_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(last_known_monthly_fee) AS last_known_monthly_fee_whole,
        CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic
    FROM filtered
    WHERE end_date > TO_DATE('2023-09-18')
      AND next_monthly_fee IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY is_enterprise, id) AS dedup_rank
    FROM filtered_subscriptions
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    end_date,
    next_monthly_fee,
    prev_monthly_fee,
    row_seq,
    last_known_monthly_fee,
    end_date_year,
    is_enterprise,
    last_known_monthly_fee_whole
FROM latest
ORDER BY is_enterprise DESC NULLS FIRST
