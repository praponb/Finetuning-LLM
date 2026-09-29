WITH metrics AS (
    SELECT
        *,
        LEAD(credit_limit) OVER w_grp AS next_credit_limit,
        LEAD(age) OVER w_grp AS next_age,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM customers
    WINDOW w_grp AS (PARTITION BY city ORDER BY credit_limit, id)
)
SELECT
    id,
    name,
    email,
    country,
    credit_limit,
    age,
    next_credit_limit,
    next_age,
    CASE WHEN name RLIKE '^(Jack Taylor|David Lee )$' THEN 'PRIORITY' ELSE 'CORE' END AS name_zone
FROM metrics
