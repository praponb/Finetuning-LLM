WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC, id ASC) AS rn
        FROM promotions
    ) t
    WHERE rn = 1
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct
FROM ranked
ORDER BY promo_code ASC NULLS FIRST;
