WITH with_windows AS (
    SELECT
        id,
        plan,
        monthly_fee,
        status,
        CUME_DIST() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_cume_dist,
        NTILE(3) OVER (ORDER BY monthly_fee, id) AS monthly_fee_bucket
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
valid AS (
    SELECT
        id,
        plan,
        monthly_fee,
        status,
        monthly_fee_cume_dist,
        monthly_fee_bucket,
        row_num,
        FLOOR(monthly_fee_bucket) AS monthly_fee_bucket_whole,
        CEIL(monthly_fee_bucket) AS monthly_fee_bucket_whole_2
    FROM latest
    WHERE status IN ('EXPIRED', 'ACTIVE')
),
cleaned AS (
    SELECT *, FLOOR(monthly_fee_bucket_whole_2) AS monthly_fee_bucket_whole_2_whole
    FROM valid
    WHERE monthly_fee_bucket_whole_2 > 39
)
SELECT
    id,
    plan,
    monthly_fee,
    status,
    monthly_fee_cume_dist,
    monthly_fee_bucket,
    row_num,
    monthly_fee_bucket_whole,
    CASE WHEN status RLIKE '^(ACTIVE|PAUSED)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS status_zone
FROM cleaned
ORDER BY id DESC NULLS LAST
LIMIT 20;
