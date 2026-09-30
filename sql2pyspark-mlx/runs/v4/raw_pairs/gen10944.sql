SELECT dept_name, location FROM departments WHERE budget <= 2138000 UNION SELECT dept_name, location FROM departments WHERE budget > 2304000 ORDER BY location DESC;
