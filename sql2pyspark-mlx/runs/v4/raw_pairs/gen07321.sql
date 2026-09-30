WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) AS rn
        FROM promotions
    ) x
    WHERE rn = 1
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    rn
FROM deduped
ORDER BY promo_code DESC
