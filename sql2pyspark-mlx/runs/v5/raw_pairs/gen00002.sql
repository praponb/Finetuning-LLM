WITH base AS (
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        CASE WHEN city IN ('Berlin', 'Singapore') THEN 'Core' ELSE 'Other' END AS city_group,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM customers
    WHERE signup_date >= DATE '2023-05-01'
)
SELECT *
FROM (
    SELECT
        city,
        segment,
        country,
        age
    FROM base
) src
PIVOT (
    SUM(age)
    FOR country IN ('DE' AS de, 'TH' AS th)
)
