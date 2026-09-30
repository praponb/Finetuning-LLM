SELECT
    id,
    segment,
    signup_date,
    ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS row_num
FROM customers
WHERE country NOT IN ('TH', 'UK')
