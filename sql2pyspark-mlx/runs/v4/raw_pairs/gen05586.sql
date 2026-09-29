WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, value
),
lines AS (
    SELECT
        id,
        value,
        element
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
cleaned AS (
    SELECT
        *,
        TRIM(element) AS clean_element,
        LENGTH(element) AS element_length,
        COALESCE(element, 'NONE') AS element_filled
    FROM lines
    WHERE element IS NOT NULL
      AND element != 'VIP'
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS element_length_cume_dist,
        MAX(element_length) OVER (PARTITION BY element) AS element_max_element_length,
        CUME_DIST() OVER (PARTITION BY element ORDER BY element_length DESC NULLS FIRST) AS element_length_cume_dist_2
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY element ORDER BY element_length DESC NULLS LAST)
)
SELECT
    CASE WHEN element_length_cume_dist >= 0.65 THEN 'LARGE' ELSE 'SMALL' END AS element_length_cume_dist_tier,
    element,
    SIZE(COLLECT_SET(element)) AS num_elements,
    ROUND(SUM(element_length) / NULLIF(COUNT(CASE WHEN element = 'WELCOME5' THEN 1 END), 0), 2) AS element_length_per_welcome5
FROM enriched
GROUP BY CASE WHEN element_length_cume_dist >= 0.65 THEN 'LARGE' ELSE 'SMALL' END, element
ORDER BY element ASC
