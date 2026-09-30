WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        country,
        city,
        is_active,
        credit_limit,
        age,
        ADD_MONTHS(signup_date, 12) AS signup_date_plus_months
    FROM customers
    WHERE signup_date > DATE '2024-02-01'
      AND city IS NOT NULL
      AND credit_limit < 4500
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        country,
        city,
        is_active,
        credit_limit,
        age,
        signup_date_plus_months,
        NTILE(5) OVER (PARTITION BY city ORDER BY age, id) AS age_bucket,
        NTILE(4) OVER w_part AS age_bucket_2,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY city) AS city_stddev_samp_credit_limit
    FROM base
    WINDOW w_part AS (ORDER BY age, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(age_bucket_2, 0) AS age_bucket_2_filled,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM enriched
    WHERE city = 'Berlin'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age_bucket DESC, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
)
SELECT
    id,
    name,
    country,
    is_active,
    credit_limit,
    signup_date_plus_months,
    age_bucket,
    age_bucket_2,
    city_stddev_samp_credit_limit,
    CASE WHEN name RLIKE '^(Pim Chaiyaporn|Grace Kim)$' THEN 'GROUP_B' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS name_zone
FROM ranked;
