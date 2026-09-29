WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END AS is_consumer
    FROM customers
    WHERE segment IS NOT NULL
      AND segment IS NOT DISTINCT FROM 'Corporate'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER w_recent AS is_consumer_bucket,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) AS row_seq,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM base
    WHERE is_active = FALSE
    WINDOW w_recent AS (ORDER BY is_consumer, id), w_grp AS (PARTITION BY country ORDER BY age, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        row_seq,
        event_seq,
        COALESCE(age, 0) AS age_filled,
        event_seq * age AS event_seq_times_age
    FROM with_windows
    WHERE signup_date > TO_DATE('2023-05-31')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        row_seq,
        event_seq,
        event_seq_times_age,
        IF(is_active, 'Yes', 'No') AS is_active_label
    FROM filtered
    WHERE signup_date >= TO_DATE('2023-04-20')
      AND segment IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM valid
    INTERSECT
    SELECT id, city
    FROM customers
    WHERE age > 43
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.id = k.id AND s.city = k.city
)
SELECT
    id,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    row_seq
FROM verified
ORDER BY signup_date NULLS FIRST;
