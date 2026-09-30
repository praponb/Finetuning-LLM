WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(starts_on, 'MMM yyyy') AS starts_on_label,
        DATE_FORMAT(ends_on, 'MMM yyyy') AS ends_on_label,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE ends_on > DATE '2023-11-29'
      AND promo_code = 'SPRING10'
)
-- QUALIFY-style window filter
SELECT
    id,
    promo_code,
    ends_on_label,
    channel_filled
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS rn
    FROM cleaned
) ranked
WHERE rn = 1;
