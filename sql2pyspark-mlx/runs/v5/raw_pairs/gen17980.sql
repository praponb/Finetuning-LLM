WITH valid AS (
    SELECT
        *,
        DATE_TRUNC('YEAR', opened_date) AS opened_date_period,
        LOWER(store_name) AS store_name_lower,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE opened_date >= TO_DATE('2023-07-25')
      AND region IS DISTINCT FROM 'West'
),
with_latest AS (
    SELECT
        s.id,
        s.city,
        s.region,
        s.opened_date,
        s.sqft,
        s.opened_date_period,
        s.store_name_lower,
        latest.order_amount,
        latest.order_discount
    FROM valid s
    JOIN LATERAL (
            SELECT c.amount AS order_amount, c.discount AS order_discount
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date_period
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    SELECT
        *,
        MIN(order_amount) OVER w_ordered AS running_min_order_amount,
        MAX(order_amount) OVER w_ordered AS running_max_order_amount,
        SUM(order_amount) OVER w_ordered AS running_sum_order_amount,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist
    FROM with_latest
    WINDOW w_ordered AS (PARTITION BY region, city ORDER BY opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_stores AS (
    SELECT
        id,
        opened_date,
        sqft,
        opened_date_period,
        order_amount,
        running_min_order_amount,
        running_sum_order_amount,
        sqft_cume_dist,
        LAG(order_discount) OVER w_recent AS prev_order_discount,
        LEAD(order_discount) OVER w_recent AS next_order_discount,
        LAG(sqft_cume_dist) OVER w_recent AS prev_sqft_cume_dist,
        LEAD(sqft_cume_dist) OVER w_recent AS next_sqft_cume_dist,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY city, region ORDER BY running_sum_order_amount, id)
),
cleaned AS (
    SELECT
        id,
        opened_date,
        sqft,
        opened_date_period,
        order_amount,
        running_sum_order_amount,
        sqft_cume_dist,
        prev_order_discount,
        next_order_discount,
        prev_sqft_cume_dist,
        event_seq,
        ROUND(next_order_discount * 1.1, 2) AS adjusted_next_order_discount
    FROM with_windows_stores
    WHERE opened_date >= DATE '2023-06-23'
)
SELECT
    CASE WHEN sqft >= 17023 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    (UNIX_TIMESTAMP(MAX(opened_date_period)) - UNIX_TIMESTAMP(MIN(opened_date_period))) / 60 AS span_minutes,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned
GROUP BY sqft_tier
HAVING (UNIX_TIMESTAMP(MAX(opened_date_period)) - UNIX_TIMESTAMP(MIN(opened_date_period))) / 60 < 1000000
ORDER BY distinct_ids DESC;
