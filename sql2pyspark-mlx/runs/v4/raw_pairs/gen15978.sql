WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        COALESCE(age, 0) AS age_filled,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM customers
    WHERE email IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.customer_id IN (9, 35)
        )
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.discount
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT segment, country
    FROM with_latest
    INTERSECT ALL
    SELECT segment, country
    FROM customers
    WHERE YEAR(signup_date) = 2024
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN kept_keys k ON s.segment = k.segment AND s.country = k.country
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM kept_rows
    WHERE segment IS NOT NULL
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    city,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
    ROUND(AVG(discount), 2) AS avg_discount,
    COUNT(*) AS cnt,
    COUNT(*) AS row_count
FROM cleaned
GROUP BY 1, 2
ORDER BY new_york_rows DESC
