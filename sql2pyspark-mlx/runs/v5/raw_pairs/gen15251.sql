WITH valid AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM suppliers
    WHERE country = 'TH'
),
metrics AS (
    SELECT
        *,
        LAG(is_singapore, 2) OVER (PARTITION BY city, country ORDER BY is_singapore ASC, id ASC) AS prev_is_singapore,
        LAG(is_singapore, 2) OVER w_hist AS prev_is_singapore_2
    FROM valid
    WINDOW w_hist AS (PARTITION BY city ORDER BY is_singapore ASC, id ASC)
),
metrics_suppliers AS (
    SELECT
        *,
        LEAD(prev_is_singapore) OVER w_recent AS next_prev_is_singapore,
        LAG(prev_is_singapore) OVER w_recent AS prev_prev_is_singapore,
        LAG(is_singapore) OVER w_recent AS prev_is_singapore_3,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM metrics
    WINDOW w_recent AS (PARTITION BY city ORDER BY prev_is_singapore_2 ASC, id ASC)
),
with_windows AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        is_singapore,
        city_filled,
        next_prev_is_singapore,
        prev_prev_is_singapore,
        event_seq,
        LAST_VALUE(prev_prev_is_singapore) IGNORE NULLS OVER w_part AS last_known_prev_prev_is_singapore,
        SUM(prev_is_singapore_2) OVER w_recent AS cumulative_prev_is_singapore_2
    FROM metrics_suppliers
    WHERE prev_is_singapore_3 = 34
    WINDOW w_part AS (ORDER BY prev_prev_is_singapore ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY country ORDER BY prev_is_singapore_3 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT *
FROM (
    SELECT
        country,
        city,
        prev_prev_is_singapore
    FROM with_windows
) src
PIVOT (
    SUM(prev_prev_is_singapore)
    FOR city IN ('Tokyo' AS tokyo, 'Singapore' AS singapore)
)
