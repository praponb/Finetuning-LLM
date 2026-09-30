WITH with_windows AS (
    SELECT
        *,
        NTILE(5) OVER w_recent AS monthly_fee_bucket,
        LAST_VALUE(monthly_fee, TRUE) OVER w_ordered AS last_known_monthly_fee
    FROM subscriptions
    WINDOW w_recent AS (PARTITION BY plan ORDER BY monthly_fee DESC, id), w_ordered AS (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY last_known_monthly_fee DESC NULLS LAST, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
ranked AS (
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        monthly_fee_bucket,
        rank_in_group
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS FIRST) = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee_bucket DESC, id) AS row_num
    FROM ranked
    QUALIFY row_num = 1
),
wide AS (
    SELECT *
    FROM (
        SELECT
            plan,
            status,
            monthly_fee_bucket
        FROM latest
    ) src
    PIVOT (
        AVG(monthly_fee_bucket) AS value_sum
        FOR status IN ('PAUSED' AS paused, 'EXPIRED' AS expired)
    )
)
SELECT
    plan,
    paused,
    expired
FROM wide;
