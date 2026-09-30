with kept_keys as (
    select id, dept_name
    from departments
    intersect
    select id, dept_name
    from departments
    where dept_name = 'Legal'
),
verified as (
    select s.*
    from departments s
    inner join kept_keys k on s.id = k.id and s.dept_name = k.dept_name
)
select
    id,
    dept_name,
    budget,
    case when dept_name rlike '^(Legal|HR)$' then 'EXTENDED' when dept_name rlike '^(Engineering|Sales)$' then 'GROUP_B' else 'GROUP_A' end as dept_name_zone
from verified
order by id;
