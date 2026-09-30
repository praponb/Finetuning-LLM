WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS pos FROM customers) SELECT id, segment, signup_date FROM ranked WHERE pos = 1 ORDER BY signup_date
