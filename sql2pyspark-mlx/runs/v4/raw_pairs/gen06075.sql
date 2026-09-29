WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_FORMAT(ends_on, 'yyyy-MM') ends_on_label,
        DATEDIFF(CURRENT_DATE(), starts_on) days_since_starts_on
    FROM promotions
    WHERE starts_on > DATE '2023-04-17'
      AND channel IS NOT NULL
),
kept_keys AS (
    SELECT channel
    FROM valid
    EXCEPT
    SELECT channel
    FROM promotions
    WHERE starts_on BETWEEN '2023-03-01' AND '2024-08-01'
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.channel = k.channel
),
cleaned AS (
    SELECT
        *,
        UPPER(promo_code) promo_code_upper,
        COALESCE(channel, 'N/A') resolved_channel
    FROM verified
    WHERE ends_on > TO_DATE('2023-08-10')
      AND channel IS NOT NULL
)
SELECT
    channel,
    promo_code,
    starts_on,
    ends_on,
    ends_on_label,
    days_since_starts_on,
    CASE WHEN promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS promo_code_zone
FROM cleaned
ORDER BY starts_on DESC NULLS LAST
