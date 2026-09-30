select
    d.dept_name,
    d.location,
    t.level_list,
    t.row_count
from departments d
left join (select dept_id, sort_array(collect_set(level)) as level_list, count(*) as row_count
    from employees
    where hire_date <= '2024-11-14'
    group by dept_id) t on d.id = t.dept_id;
