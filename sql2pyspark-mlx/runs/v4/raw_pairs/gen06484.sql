SELECT id, credit_limit, age, ROW_NUMBER() OVER (ORDER BY credit_limit DESC, id) AS row_num, RANK() OVER (ORDER BY age DESC) AS rnk FROM customers ORDER BY row_num DESC
