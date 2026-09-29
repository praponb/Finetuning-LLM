select *
from (
    select dept_id, count(distinct salary) unique_levels, round((100 * sum(case when level = 'L2' then 1 else 0 end)) / count(*), 1) pct_l2
    from employees
    group by dept_id
) sub
where unique_levels < 8
