WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS element
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element) AS element_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_cume_dist_2
    FROM exploded
),
base AS (
    SELECT
        id,
        plan,
        effective_from,
        element_cume_dist,
        element_cume_dist_2,
        INITCAP(TRIM(plan)) AS plan_title
    FROM with_windows
    WHERE element_cume_dist_2 >= 0.52
)
SELECT *
FROM (
    SELECT
        plan,
        element_cume_dist_2,
        element_cume_dist
    FROM base
) src
UNPIVOT (
    val FOR metric IN (element_cume_dist_2 AS ELEMENT_CUME_DIST_2_VAL, element_cume_dist AS ELEMENT_CUME_DIST)
)
