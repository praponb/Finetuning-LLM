select dept_name, max(budget) as max_budget from departments where budget between 3040000 and 3990000 group by dept_name having max(budget) >= 1638000 order by dept_name;
