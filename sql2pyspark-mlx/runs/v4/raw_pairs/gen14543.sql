SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST, id) AS row_num
FROM departments
QUALIFY row_num = 1
