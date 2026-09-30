select location, concat_ws(', ', sort_array(collect_set(dept_name))) as dept_name_values, min(budget) as min_budget, count(*) as department_count from departments group by 1;
