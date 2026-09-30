WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        CASE WHEN status IN ('ACTIVE', 'PAUSED') THEN 'Group A' ELSE 'Other' END AS status_group,
        FLOOR(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE start_date >= DATE '2023-06-11'
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        status_group,
        LAST_DAY(start_date) AS start_date_month_end
    FROM base
    WHERE status IN ('PAUSED', 'ACTIVE', 'CANCELLED')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date_month_end, id) AS latest_rank
        FROM base_subscriptions
    ) x
    WHERE latest_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY latest_rank DESC, id) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY monthly_fee, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY latest_rank, id) AS event_seq,
    NTILE(3) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS monthly_fee_bucket
FROM ranked;
