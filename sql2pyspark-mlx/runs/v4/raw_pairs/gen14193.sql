WITH cleaned AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        TRIM(city) AS clean_city
    FROM customers
    WHERE signup_date >= TO_DATE('2023-03-28')
      AND segment IS NOT NULL
      AND age >= 31
),
latest AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY signup_date DESC, id ASC) = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY age DESC, id ASC) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
),
not_flagged AS (
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.browser = 'Safari'
        )
),
enriched AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        clean_city,
        row_num,
        NTILE(4) OVER (ORDER BY credit_limit ASC, id ASC) AS credit_limit_bucket,
        MAX(age) OVER (PARTITION BY city) AS city_max_age,
        FIRST_VALUE(city, TRUE) OVER (ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        CORR(age, credit_limit) OVER (PARTITION BY city) AS corr_age_credit_limit
    FROM not_flagged
),
scored AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        clean_city,
        row_num,
        credit_limit_bucket,
        city_max_age,
        first_known_city,
        corr_age_credit_limit,
        SUM(age) OVER (PARTITION BY segment) AS segment_sum_age,
        LEAD(city_max_age, 1) OVER w_running AS next_city_max_age,
        NTILE(5) OVER (PARTITION BY segment, city ORDER BY age ASC, id ASC) AS age_bucket
    FROM enriched
    WINDOW w_running AS (ORDER BY corr_age_credit_limit ASC, id ASC)
)
SELECT *
FROM (
    SELECT
        segment,
        city,
        age_bucket
    FROM scored
) src
PIVOT (
    AVG(age_bucket) AS value_sum
    FOR city IN ('Tokyo' AS tokyo, 'Chiang Mai' AS chiang_mai, 'New York' AS new_york)
);
