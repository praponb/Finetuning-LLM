WITH filtered AS (
    SELECT *, SUBSTRING(city, 1, 3) AS city_prefix
    FROM customers
    WHERE city IS DISTINCT FROM 'Bangkok'
      AND country <> 'TH'
),
filtered_customers AS (
    SELECT
        *,
        CONCAT(country, '/', email) AS country_email,
        COALESCE(city, 'N/A') AS resolved_city
    FROM filtered
    WHERE city IS DISTINCT FROM 'Chiang Mai'
      AND city IS NOT NULL
),
enriched AS (
    SELECT
        *,
        CORR(age, credit_limit) OVER w_grp AS corr_age_credit_limit,
        ROW_NUMBER() OVER w_recent AS event_seq,
        MIN(age) OVER w_grp AS segment_min_age
    FROM filtered_customers
    WINDOW w_grp AS (PARTITION BY segment), w_recent AS (PARTITION BY segment ORDER BY signup_date ASC, id ASC)
),
with_last AS (
    SELECT s.*, latest.browser
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_customers AS (
    SELECT
        s.*,
        x.discount,
        x.order_shipping_fee
    FROM with_last s
    JOIN LATERAL (
            SELECT c.discount, c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    country,
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    segment,
    COUNT_IF(country = 'DE') AS de_events,
    SUM(age) FILTER (WHERE browser = 'Firefox') AS firefox_total_age,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    COUNT(DISTINCT segment) AS distinct_segments,
    COUNT(*) AS event_count,
    COUNT(*) AS event_count_2,
    GROUPING_ID() AS gid
FROM with_last_customers
GROUP BY ROLLUP (country, DATE_TRUNC('MONTH', signup_date), segment)
ORDER BY event_count DESC;
