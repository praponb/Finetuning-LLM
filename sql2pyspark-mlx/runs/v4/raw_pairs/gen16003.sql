WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(age * 0.85, 2) AS adjusted_age,
        COALESCE(city, 'Unknown') AS city_filled
    FROM customers
    WHERE signup_date >= DATE '2024-01-21'
      AND country = 'US'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Running and moving aggregates
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
        age,
        adjusted_age,
        city_filled,
        CORR(age, credit_limit) OVER w_ordered AS corr_age_credit_limit,
        ROW_NUMBER() OVER w_ordered_2 AS seq_no,
        STDDEV_SAMP(adjusted_age) OVER w_ordered AS city_stddev_samp_adjusted_age
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY city), w_ordered_2 AS (PARTITION BY country ORDER BY signup_date, id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(segment)) AS segment_title,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM enriched
    WHERE signup_date > TO_DATE('2023-11-11')
),
deduped_customers AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY city_stddev_samp_adjusted_age DESC, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(adjusted_age) IGNORE NULLS OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_adjusted_age,
        SUM(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit
    FROM deduped_customers
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            city,
            segment,
            adjusted_age
        FROM scored
    ) src
    PIVOT (
        SUM(adjusted_age)
        FOR segment IN ('SMB' AS smb, 'Consumer' AS consumer, 'Corporate' AS corporate)
    )
)
SELECT
    city,
    smb,
    consumer,
    corporate
FROM pivoted;
