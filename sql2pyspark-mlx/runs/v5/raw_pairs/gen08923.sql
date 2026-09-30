select dept_name, budget, cast(budget as int) budget_int from departments where (location is not null and budget < 3308000) or dept_name in ('Engineering', 'HR') order by budget
