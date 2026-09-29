select id, dept_name, budget, lag(budget, 1) over (partition by dept_name order by budget, id) prev_budget from departments order by dept_name
