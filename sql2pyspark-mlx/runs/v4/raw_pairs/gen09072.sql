WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        age,
        REGEXP_REPLACE(name, ' ', '_') name_slug,
        LOWER(email) email_lower,
        COALESCE(segment, 'OTHER') segment_filled
    FROM customers
    WHERE signup_date >= DATE '2023-06-05'
      AND signup_date BETWEEN '2024-11-01' AND '2025-08-01'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY city, country ORDER BY age DESC) = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY segment ORDER BY age ASC NULLS FIRST) age_percent_rank,
        SUM(age) OVER (PARTITION BY city ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_age,
        MAX(age) OVER (PARTITION BY city) city_max_age,
        AVG(age) OVER (ORDER BY signup_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) age_avg_6_prior,
        SUM(age) OVER (PARTITION BY segment ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_age_2
    FROM not_flagged
),
filtered_customers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        age,
        name_slug,
        email_lower,
        age_percent_rank,
        cumulative_age,
        city_max_age,
        cumulative_age_2,
        age_percent_rank - age age_percent_rank_minus_age
    FROM enriched
    WHERE signup_date >= DATE '2023-07-12'
      AND cumulative_age BETWEEN 248 AND 308
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered_customers
    INTERSECT
    SELECT id
    FROM customers
    WHERE segment IN ('SMB', 'Consumer', 'Corporate')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_customers s
    INNER JOIN matched_keys k ON s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    signup_date,
    age,
    email_lower,
    age_percent_rank
FROM kept
ORDER BY age_percent_rank DESC NULLS LAST, signup_date ASC;
