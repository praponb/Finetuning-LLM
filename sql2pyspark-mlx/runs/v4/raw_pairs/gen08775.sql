WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE discount_pct BETWEEN 0.15 AND 0.2
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), starts_on) AS days_since_starts_on,
        CASE WHEN channel IN ('partner', 'store') THEN 'Core' ELSE 'Other' END AS channel_group,
        COALESCE(channel, 'UNKNOWN') AS channel_filled_2
    FROM valid
    WHERE starts_on > TO_DATE('2023-08-17')
      AND channel IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id ASC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(days_since_ends_on) OVER (PARTITION BY promo_code ORDER BY rn ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_ends_on_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id ASC) AS event_seq
    FROM latest
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    days_since_ends_on,
    channel_filled,
    channel_group,
    channel_filled_2
FROM scored
ORDER BY discount_pct ASC NULLS FIRST;
