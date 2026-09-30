WITH base AS (
    SELECT
        id,
        store_name,
        city,
        sqft,
        FLOOR(sqft) AS sqft_whole,
        UPPER(region) AS region_upper,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE opened_date >= TO_DATE('2023-06-18')
      AND region IS NOT NULL
)
SELECT
    s.id,
    s.store_name,
    s.city,
    s.sqft,
    s.region_upper,
    top1.order_status
FROM base s
JOIN LATERAL (
        SELECT c.status AS order_status
        FROM orders c
        WHERE c.store_id = s.id
        ORDER BY c.shipping_fee DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
