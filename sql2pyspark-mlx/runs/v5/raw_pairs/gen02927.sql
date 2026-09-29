WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END is_active,
        CONCAT(status, ', ', plan) status_plan
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-10-09')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC NULLS LAST, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY end_date NULLS LAST) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
),
deduped_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY rank_in_group DESC, id) AS rn
    FROM latest
    QUALIFY rn = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        is_active,
        status_plan,
        rank_in_group,
        latest_rank,
        rn,
        latest_rank * rank_in_group latest_rank_times_rank_in_group
    FROM deduped_subscriptions
    WHERE customer_id IS NOT NULL
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            plan,
            latest_rank_times_rank_in_group
        FROM filtered
    ) src
    PIVOT (
        SUM(latest_rank_times_rank_in_group)
        FOR plan IN ('enterprise', 'premium')
    )
)
SELECT
    status,
    enterprise,
    premium,
    CASE WHEN status RLIKE '^(PAUSED|EXPIRED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(ACTIVE|CANCELLED)$' THEN 'GROUP_B' ELSE 'CORE' END AS status_zone
FROM wide
ORDER BY status_zone NULLS FIRST;
