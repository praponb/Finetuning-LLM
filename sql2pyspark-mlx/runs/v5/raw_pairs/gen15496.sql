with with_windows as (
    select
        id,
        location,
        budget,
        lag(budget) over w_hist as prev_budget,
        lead(budget) over w_hist as next_budget,
        row_number() over w_hist as seq_no
    from departments
    where dept_name in ('Sales', 'HR', 'Engineering')
    window w_hist as (partition by dept_name order by budget, id)
)
select
    id,
    location,
    budget,
    prev_budget,
    next_budget,
    seq_no
from with_windows
order by location nulls last;
