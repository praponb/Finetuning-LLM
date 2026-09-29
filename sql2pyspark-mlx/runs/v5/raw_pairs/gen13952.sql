WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
cleaned AS (
    SELECT *, TRIM(element) AS clean_element
    FROM lines
    WHERE created_at > TO_DATE('2025-03-01')
      AND customer_id IS NOT NULL
),
with_dims AS (
    SELECT
        e.*,
        d.email,
        d.name,
        d.credit_limit
    FROM cleaned e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
),
cleaned_carts AS (
    SELECT
        id,
        created_at,
        element,
        clean_element,
        email,
        name,
        credit_limit,
        CASE WHEN credit_limit >= 14300 THEN 'High' WHEN credit_limit >= 8000 THEN 'Medium' ELSE 'Low' END AS credit_limit_tier,
        COALESCE(element, 'UNKNOWN') AS resolved_element
    FROM with_dims
    WHERE email IS NOT NULL
      AND element IS NOT NULL
),
filtered AS (
    SELECT
        id,
        element,
        clean_element,
        email,
        name,
        credit_limit,
        credit_limit_tier,
        resolved_element,
        UPPER(name) AS name_upper,
        HOUR(created_at) AS created_at_hour
    FROM cleaned_carts
    WHERE created_at >= DATE '2025-03-01'
),
cleaned_carts_2 AS (
    SELECT
        id,
        element,
        clean_element,
        name,
        credit_limit,
        credit_limit_tier,
        resolved_element,
        created_at_hour,
        ROUND(created_at_hour * 1.1, 2) AS adjusted_created_at_hour,
        COALESCE(element, 'NONE') AS element_filled
    FROM filtered
    WHERE element = 'VIP'
),
base AS (
    SELECT
        id,
        element,
        clean_element,
        resolved_element,
        created_at_hour,
        adjusted_created_at_hour,
        element_filled,
        COALESCE(created_at_hour, 0) AS created_at_hour_filled,
        COALESCE(element, 'NONE') AS resolved_element_2
    FROM cleaned_carts_2
    WHERE element IS NOT NULL
)
SELECT
    id,
    clean_element,
    resolved_element,
    created_at_hour,
    element_filled,
    created_at_hour_filled,
    resolved_element_2,
    RANK() OVER (PARTITION BY element ORDER BY created_at_hour NULLS FIRST) AS created_at_hour_rank
FROM base;
