WITH remaining_keys AS (
    -- Set difference on keys
    SELECT channel, id
    FROM promotions
    EXCEPT ALL
    SELECT channel, id
    FROM promotions
    WHERE starts_on BETWEEN '2023-05-01' AND '2024-05-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    JOIN remaining_keys k ON s.channel = k.channel AND s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        CONCAT_WS('-', channel, promo_code) AS channel_promo_code_key,
        UPPER(promo_code) AS promo_code_upper,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM reconciled
    WHERE starts_on > TO_DATE('2023-07-21')
      AND channel IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LAST_DAY(ends_on) AS ends_on_month_end,
        COALESCE(channel, 'DIRECT') AS channel_clean_2
    FROM base
    WHERE starts_on >= DATE '2023-04-29'
      AND channel IS DISTINCT FROM 'mobile'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        channel_promo_code_key,
        promo_code_upper,
        channel_clean,
        channel_clean_2,
        CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END AS is_mobile
    FROM valid
    WHERE channel IS NOT NULL
)
SELECT
    promo_code,
    starts_on,
    ends_on,
    channel_promo_code_key,
    promo_code_upper,
    channel_clean_2,
    is_mobile
FROM cleaned
