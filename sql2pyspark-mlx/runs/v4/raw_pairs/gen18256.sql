WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        credit_limit,
        age,
        TRIM(city) clean_city,
        COALESCE(segment, 'NONE') segment_filled
    FROM customers
    WHERE segment = 'Corporate'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit, id) AS rank_in_group
        FROM base
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        credit_limit,
        age,
        clean_city,
        segment_filled,
        rank_in_group,
        LEAD(credit_limit, 2) OVER (PARTITION BY country, city ORDER BY rank_in_group, id) next_credit_limit,
        CUME_DIST() OVER (PARTITION BY city ORDER BY age NULLS LAST) age_cume_dist,
        FIRST_VALUE(age) IGNORE NULLS OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_age
    FROM ranked
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.device
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.device
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY age_cume_dist DESC NULLS FIRST, id) AS dedup_rank
    FROM with_last
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        credit_limit,
        age,
        clean_city,
        segment_filled,
        rank_in_group,
        next_credit_limit,
        age_cume_dist,
        first_known_age,
        device,
        dedup_rank,
        ROUND(first_known_age / SUM(first_known_age) OVER (PARTITION BY device), 4) first_known_age_share,
        CUME_DIST() OVER w_part age_cume_dist_cume_dist,
        SUM(next_credit_limit) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_next_credit_limit
    FROM latest
    WINDOW w_part AS (PARTITION BY city ORDER BY age_cume_dist)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device,
        age_cume_dist_cume_dist,
        next_credit_limit,
        age_cume_dist
    FROM metrics
) src
UNPIVOT (
    val FOR measure IN (age_cume_dist_cume_dist AS AGE_CUME_DIST_CUME_DIST_VAL, next_credit_limit AS NEXT_CREDIT_LIMIT, age_cume_dist AS AGE_CUME_DIST)
);
