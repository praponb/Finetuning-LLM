WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(email) AS email_lower
    FROM customers
    WHERE city IS DISTINCT FROM 'New York'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(credit_limit) OVER w_part AS running_min_credit_limit,
        COUNT(credit_limit) OVER w_part AS running_count_credit_limit,
        MAX(credit_limit) OVER w_part AS running_max_credit_limit,
        AVG(credit_limit) OVER w_part AS running_avg_credit_limit,
        FIRST_VALUE(segment) IGNORE NULLS OVER w_part_2 AS first_known_segment
    FROM cleaned
    WINDOW w_part AS (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY country, segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, segment
    FROM scored
    EXCEPT
    SELECT id, segment
    FROM customers
    WHERE city IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.id = k.id AND s.segment = k.segment
),
enriched AS (
    -- Ranking and gap features
    SELECT
        name,
        email,
        country,
        credit_limit,
        email_lower,
        running_count_credit_limit,
        first_known_segment,
        COUNT(credit_limit) OVER (PARTITION BY city) AS city_count_credit_limit,
        COUNT(running_avg_credit_limit) OVER (PARTITION BY city) AS city_count_running_avg_credit_limit
    FROM reconciled
    WHERE NOT is_active
),
cleaned_customers AS (
    -- Keep recent, valid rows
    SELECT
        name,
        email,
        country,
        credit_limit,
        email_lower,
        running_count_credit_limit,
        city_count_credit_limit + city_count_running_avg_credit_limit AS city_count_credit_limit_plus_city_count_running_avg_credit_limit,
        CAST(city_count_running_avg_credit_limit AS DOUBLE) AS city_count_running_avg_credit_limit_str
    FROM enriched
    WHERE country IN ('US', 'UK')
),
scored_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER w_part AS city_count_running_avg_credit_limit_str_percent_rank,
        PERCENT_RANK() OVER w_grp AS credit_limit_percent_rank,
        ROUND(running_count_credit_limit / SUM(running_count_credit_limit) OVER (PARTITION BY country), 4) AS running_count_credit_limit_share,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY city_count_running_avg_credit_limit_str DESC NULLS LAST) AS city_count_running_avg_credit_limit_str_dense_rank,
        DENSE_RANK() OVER w_hist AS running_count_credit_limit_dense_rank
    FROM cleaned_customers
    WINDOW w_part AS (PARTITION BY country ORDER BY city_count_running_avg_credit_limit_str NULLS LAST), w_grp AS (PARTITION BY country ORDER BY credit_limit NULLS FIRST), w_hist AS (PARTITION BY country ORDER BY running_count_credit_limit NULLS LAST)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        name,
        email,
        country,
        credit_limit,
        email_lower,
        city_count_credit_limit_plus_city_count_running_avg_credit_limit,
        credit_limit_percent_rank,
        city_count_running_avg_credit_limit_str_dense_rank,
        running_count_credit_limit_dense_rank,
        COALESCE(credit_limit, 0) AS credit_limit_filled,
        CEIL(running_count_credit_limit) AS running_count_credit_limit_whole
    FROM scored_customers
    WHERE country <> 'DE'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit_percent_rank >= 0.74 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_percent_rank_tier,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count_2,
    MIN(running_count_credit_limit_whole) AS min_running_count_credit_limit_whole,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM valid
GROUP BY CASE WHEN credit_limit_percent_rank >= 0.74 THEN 'HIGH' ELSE 'LOW' END
ORDER BY th_count_2 NULLS LAST
