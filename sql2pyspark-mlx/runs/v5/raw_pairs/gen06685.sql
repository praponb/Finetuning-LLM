WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, attr_value
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.coupon_codes,
        e.k,
        e.attr_value,
        d.segment,
        d.city,
        d.age
    FROM lines e
    LEFT JOIN customers d
        ON e.customer_id = d.id
    WHERE d.is_active
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        coupon_codes,
        segment,
        age,
        CASE WHEN k IN ('device', 'utm_source') THEN 'Priority' ELSE 'Other' END k_group
    FROM joined
    WHERE city = 'London'
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ARRAY_JOIN(coupon_codes, ', ') coupon_codes_text,
        COALESCE(segment, 'DIRECT') segment_filled
    FROM filtered
    WHERE segment IS NOT NULL
      AND ARRAY_CONTAINS(coupon_codes, 'VIP')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(segment) segment_upper
    FROM filtered_carts
    WHERE segment IS DISTINCT FROM 'Consumer'
)
-- Aggregate with conditional metrics
SELECT
    segment,
    SUM(CASE WHEN segment = 'SMB' THEN age ELSE 0 END) AS smb_age,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    SUM(CASE WHEN segment = 'SMB' THEN age ELSE 0 END) AS smb_age_2
FROM cleaned
GROUP BY segment
ORDER BY smb_age DESC NULLS LAST;
