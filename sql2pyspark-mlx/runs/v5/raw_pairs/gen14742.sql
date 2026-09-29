SELECT
    manager_id,
    CASE WHEN salary >= 154000 THEN 'high' WHEN salary >= 55000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END) AS analyst_count,
    SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END) AS designer_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
FROM employees
GROUP BY 1, 2;
