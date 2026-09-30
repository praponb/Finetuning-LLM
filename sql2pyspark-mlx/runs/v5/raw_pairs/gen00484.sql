SELECT
    *,
    LEAD(age) OVER (PARTITION BY country, city ORDER BY credit_limit, id) next_age,
    LAG(age) OVER (PARTITION BY country, city ORDER BY credit_limit, id) prev_age,
    LEAD(credit_limit) OVER (PARTITION BY country, city ORDER BY credit_limit, id) next_credit_limit,
    LAG(credit_limit) OVER (PARTITION BY country, city ORDER BY credit_limit, id) prev_credit_limit,
    ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY credit_limit, id) row_seq,
    NTILE(3) OVER (PARTITION BY segment, country ORDER BY credit_limit DESC, id) credit_limit_bucket
FROM customers;
