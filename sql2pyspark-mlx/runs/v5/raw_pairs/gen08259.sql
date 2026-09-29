WITH filtered AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        CASE WHEN promo_code = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10,
        CONCAT_WS(',', channel, promo_code) AS channel_promo_code_key
    FROM promotions
    WHERE ends_on >= DATE '2023-10-24'
)
SELECT
    id,
    promo_code,
    starts_on,
    is_spring10,
    channel_promo_code_key
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) = 1;
