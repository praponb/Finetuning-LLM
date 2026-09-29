with department_summary as (
    select dept_id, concat_ws(', ', sort_array(collect_set(level))) as level_values
    from employees
    group by dept_id
)
select d.id, d.location, ds.level_values
from departments d
left outer join department_summary ds on d.id = ds.dept_id
order by ds.level_values;
