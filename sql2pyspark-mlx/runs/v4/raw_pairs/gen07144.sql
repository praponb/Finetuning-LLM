SELECT dept_name, SUM(budget) total_budget, COUNT(*) cnt, RANK() OVER (ORDER BY SUM(budget) DESC) AS rnk FROM departments GROUP BY dept_name;
