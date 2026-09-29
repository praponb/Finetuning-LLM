SELECT dept_name, ROUND(STDDEV(budget), 2) stddev_budget FROM departments GROUP BY dept_name;
