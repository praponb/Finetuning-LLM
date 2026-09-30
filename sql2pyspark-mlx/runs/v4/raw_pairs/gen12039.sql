with scored as (
    -- Running and moving aggregates
    select
        *,
        first_value(budget) ignore nulls over w_ordered as first_known_budget,
        ntile(5) over (order by budget desc, id) as budget_bucket
    from departments
    window w_ordered as (order by budget, id rows between unbounded preceding and current row)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by location order by first_known_budget, id) as rank_in_group
        from scored
    ) x
    where rank_in_group = 1
),
with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.level,
        latest.salary
    from latest s
    join lateral (
            select c.level, c.salary
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) latest on true
)
-- Native SQL PIVOT
select *
from (
    select
        location,
        dept_name,
        budget_bucket
    from with_last
) src
pivot (
    max(budget_bucket) as value_sum
    for dept_name in ('HR', 'Engineering', 'Legal')
);
