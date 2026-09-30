WITH base AS (
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
        REPLACE(email, ' ', '-') AS email_slug,
        CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END AS is_consumer,
        COALESCE(segment, 'NONE') AS resolved_segment
    FROM customers
    WHERE signup_date > DATE '2023-04-05'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email_slug,
        resolved_segment,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY credit_limit DESC, id) AS row_seq,
        COUNT(credit_limit) OVER (PARTITION BY country) AS country_count_credit_limit,
        AVG(is_consumer) OVER (PARTITION BY city ORDER BY is_consumer, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS is_consumer_avg_2_prior,
        FIRST_VALUE(city, TRUE) OVER w_recent AS first_known_city
    FROM base
    WHERE is_active
    WINDOW w_recent AS (PARTITION BY segment, city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, REPLACE(name, ' ', '-') AS name_slug
    FROM with_windows
    WHERE country_count_credit_limit >= 5
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT ALL
    SELECT id
    FROM customers
    WHERE name IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        name,
        is_consumer_avg_2_prior,
        first_known_city,
        DENSE_RANK() OVER (ORDER BY is_consumer_avg_2_prior DESC NULLS LAST) AS is_consumer_avg_2_prior_dense_rank,
        PERCENT_RANK() OVER (ORDER BY country_count_credit_limit NULLS FIRST) AS country_count_credit_limit_percent_rank,
        RANK() OVER (ORDER BY row_seq DESC NULLS FIRST) AS row_seq_rank,
        RANK() OVER (ORDER BY country_count_credit_limit DESC) AS country_count_credit_limit_rank
    FROM reconciled
)
SELECT
    is_consumer_avg_2_prior,
    country_count_credit_limit_percent_rank,
    row_seq_rank,
    country_count_credit_limit_rank
FROM scored
ORDER BY country_count_credit_limit_rank DESC NULLS FIRST;
