SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS seq_no,
    NTILE(3) OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id) AS budget_bucket
FROM departments
