WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
joined AS (
    SELECT
        s.*,
        b.country,
        b.segment
    FROM lines s
    JOIN customers b
        ON s.customer_id = b.id
),
with_dims AS (
    SELECT
        src.*,
        c.segment AS customer_segment,
        c.city
    FROM joined src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
SELECT
    created_at,
    code,
    segment,
    customer_segment,
    city,
    CASE WHEN city RLIKE '^(Tokyo|Singapore)$' THEN 'GROUP_B' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS city_zone
FROM with_dims
ORDER BY city_zone DESC;
