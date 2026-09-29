SELECT id, budget FROM departments WHERE budget >= (SELECT MAX(budget) FROM departments WHERE dept_name IN ('Engineering', 'HR', 'Finance')) ORDER BY id DESC, budget DESC
