WITH ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY signup_date DESC) AS rn FROM customers WHERE segment IN ('SMB', 'Consumer')) SELECT * FROM ranked WHERE rn <= 1;
