WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        CONCAT(city, ', ', region) AS city_region,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE region IS DISTINCT FROM 'East'
      AND opened_date > DATE '2023-02-17'
),
latest_match AS (
    SELECT
        s.id,
        s.city,
        s.city_region,
        s.region_clean,
        latest.channel
    FROM filtered s
    JOIN LATERAL (
            SELECT c.channel
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
remaining_keys AS (
    SELECT id
    FROM latest_match
    EXCEPT
    SELECT id
    FROM stores
    WHERE store_name LIKE 'Ma%'
),
reconciled AS (
    SELECT s.*
    FROM latest_match s
    INNER JOIN remaining_keys k ON s.id = k.id
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.status IN ('SHIPPED', 'COMPLETED')
        )
)
SELECT
    id,
    city,
    city_region,
    region_clean,
    channel,
    CASE WHEN city RLIKE '^(London|Singapore)$' THEN 'OTHER_ZONE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS city_zone
FROM eligible
ORDER BY id DESC NULLS FIRST, city_zone;
