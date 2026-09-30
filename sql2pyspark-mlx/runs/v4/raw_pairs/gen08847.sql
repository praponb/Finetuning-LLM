WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(city) AS city_upper,
        COALESCE(credit_limit, 0) AS credit_limit_filled
    FROM customers
    WHERE LENGTH(email) >= 9
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.session_country
    FROM valid s
    JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.session_device,
        latest.browser
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.device AS session_device, c.browser
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date ASC, id ASC) AS event_seq,
        SUM(age) OVER (ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        LEAD(credit_limit, 2) OVER (PARTITION BY city ORDER BY signup_date ASC, id ASC) AS next_credit_limit,
        COUNT(age) OVER (PARTITION BY country) AS country_count_age
    FROM latest_match
),
matched_keys AS (
    -- Set difference on keys
    SELECT city
    FROM scored
    EXCEPT ALL
    SELECT city
    FROM customers
    WHERE age BETWEEN 52 AND 66
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.city = k.city
),
kept_keys AS (
    -- Set difference on keys
    SELECT segment, id
    FROM reconciled
    EXCEPT ALL
    SELECT segment, id
    FROM customers
    WHERE country NOT IN ('UK', 'SG', 'JP')
),
reconciled_customers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN kept_keys k ON s.segment = k.segment AND s.id = k.id
),
scored_customers AS (
    -- Ranking and gap features
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
        city_upper,
        credit_limit_filled,
        session_country,
        session_device,
        browser,
        event_seq,
        running_age,
        next_credit_limit,
        country_count_age,
        MAX(age) OVER (PARTITION BY session_device) AS session_device_max_age,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY session_country), 4) AS event_seq_share,
        PERCENT_RANK() OVER w_running AS credit_limit_filled_percent_rank,
        RANK() OVER (ORDER BY event_seq DESC) AS event_seq_rank,
        RANK() OVER (PARTITION BY session_device ORDER BY running_age DESC) AS running_age_rank
    FROM reconciled_customers
    WINDOW w_running AS (PARTITION BY session_country, city ORDER BY credit_limit_filled DESC NULLS LAST)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_country,
        segment,
        event_seq
    FROM scored_customers
) src
PIVOT (
    MAX(event_seq)
    FOR segment IN ('Consumer', 'Corporate')
);
