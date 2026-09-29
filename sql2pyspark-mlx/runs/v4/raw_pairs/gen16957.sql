SELECT
    d.dept_name,
    e.email,
    e.level,
    e.job_title
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
WHERE e.job_title NOT IN ('Director', 'Manager') AND d.budget > 1215000;
