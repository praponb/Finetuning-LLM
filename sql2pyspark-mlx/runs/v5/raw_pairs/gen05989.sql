WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(city, 'Unknown') AS city_filled,
        COALESCE(city, 'N/A') AS resolved_city
    FROM customers
    WHERE signup_date >= DATE '2023-05-10'
      AND city IS DISTINCT FROM 'New York'
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.status,
        latest.order_channel
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.status, c.channel AS order_channel
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        city_filled,
        resolved_city,
        status,
        CASE WHEN segment IN ('Consumer', 'Corporate') THEN 'Priority' ELSE 'Other' END AS segment_group,
        COALESCE(segment, 'N/A') AS resolved_segment
    FROM with_last
    WHERE signup_date >= DATE '2023-03-09'
      AND city IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        is_active,
        credit_limit,
        city_filled,
        resolved_city,
        segment_group,
        resolved_segment,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 4) AS credit_limit_share,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        NTILE(5) OVER w_recent AS credit_limit_bucket
    FROM base
    WINDOW w_recent AS (PARTITION BY city ORDER BY credit_limit DESC, id)
)
SELECT
    id,
    country,
    city_filled,
    segment_group,
    resolved_segment,
    credit_limit_share,
    credit_limit_bucket,
    CASE WHEN country RLIKE '^(TH|SG)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS country_zone
FROM with_windows
ORDER BY credit_limit_share DESC NULLS LAST;
