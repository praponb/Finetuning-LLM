WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN city IN ('London', 'Tokyo') THEN 'Priority' ELSE 'Other' END AS city_group
    FROM customers
    WHERE city IS NOT NULL
      AND UPPER(email) LIKE '%IAM%'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.store_id IS NOT NULL
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        city_group,
        LOWER(city) AS city_lower,
        LAST_DAY(signup_date) AS signup_date_month_end,
        COALESCE(segment, 'N/A') AS resolved_segment
    FROM eligible
    WHERE city IN ('Bangkok', 'Tokyo')
),
matched_keys AS (
    -- Set difference on keys
    SELECT segment
    FROM cleaned
    EXCEPT ALL
    SELECT segment
    FROM customers
    WHERE YEAR(signup_date) >= 2024
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.segment = k.segment
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT segment
    FROM reconciled
    INTERSECT ALL
    SELECT segment
    FROM customers
    WHERE credit_limit IS NOT NULL
),
reconciled_customers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN remaining_keys k ON s.segment = k.segment
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY age ASC NULLS FIRST) AS age_dense_rank,
        DENSE_RANK() OVER (ORDER BY age ASC) AS age_dense_rank_2
    FROM reconciled_customers
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN age_dense_rank_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS age_dense_rank_2_tier,
        city,
        ROUND(AVG(age), 2) AS avg_age,
        COUNT_IF(country = 'US') AS us_events,
        SIZE(COLLECT_SET(city)) AS num_cities,
        COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
        ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS age_per_singapore,
        SUM(CASE WHEN country = 'SG' THEN age_dense_rank ELSE 0 END) AS sg_age_dense_rank
    FROM scored
    GROUP BY CUBE (CASE WHEN age_dense_rank_2 >= 7 THEN 'HIGH' ELSE 'LOW' END, city)
    HAVING COUNT_IF(country = 'US') > 1
)
SELECT
    age_dense_rank_2_tier,
    city,
    avg_age,
    num_cities,
    berlin_rows,
    sg_age_dense_rank
FROM stats;
