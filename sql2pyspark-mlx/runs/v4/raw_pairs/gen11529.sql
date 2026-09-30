SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) event_seq,
    AVG(age) OVER w_part age_avg_5
FROM customers
WINDOW w_part AS (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW);
