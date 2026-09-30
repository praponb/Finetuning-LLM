SELECT id, budget, INITCAP(TRIM(location)) AS location_title
FROM departments
WHERE dept_name IN ('HR', 'Marketing') AND dept_name = 'Engineering';
