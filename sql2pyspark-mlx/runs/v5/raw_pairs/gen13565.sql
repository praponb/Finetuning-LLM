WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, v
),
lines AS (
    SELECT
        id,
        customer_id,
        k,
        element
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
joined AS (
    SELECT
        a.*,
        p.name,
        p.email
    FROM lines a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
enriched AS (
    SELECT
        src.*,
        c.email AS customer_email,
        c.name AS customer_name,
        c.city
    FROM joined src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
joined_carts AS (
    SELECT
        e.customer_id,
        e.k,
        e.element,
        e.name,
        e.email,
        e.customer_email,
        e.city,
        c.segment
    FROM enriched e
    INNER JOIN customers c
        ON e.customer_id = c.id
    WHERE c.name LIKE 'I%'
),
filtered AS (
    SELECT
        *,
        UPPER(TRIM(customer_email)) AS customer_email_normalized,
        COALESCE(city, 'NONE') AS city_filled
    FROM joined_carts
    WHERE k IS NOT NULL
      AND LOWER(name) LIKE '%enr%'
)
SELECT
    segment,
    element,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE k = 'device') AS device_rows,
    COUNT(CASE WHEN element = 'VIP' THEN 1 END) AS vip_count,
    COUNT_IF(city = 'Berlin') AS berlin_events,
    GROUPING(segment) AS grouping_level
FROM filtered
GROUP BY GROUPING SETS ((segment, element), (segment))
ORDER BY berlin_events DESC
