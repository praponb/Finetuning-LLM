with cleaned as (
    select
        *,
        case when dept_name = 'Legal' then 1 else 0 end as is_legal,
        upper(location) as location_upper,
        coalesce(location, 'N/A') as resolved_location
    from departments
    where location <> 'London'
),
metrics as (
    select
        *,
        min(is_legal) over w_grp as last7_min_is_legal,
        sum(is_legal) over w_grp as last7_sum_is_legal,
        count(is_legal) over w_grp as last7_count_is_legal,
        round(budget / sum(budget) over (partition by location), 4) as budget_share
    from cleaned
    window w_grp as (partition by dept_name order by budget, id rows between 6 preceding and current row)
),
metrics_departments as (
    select
        *,
        lag(budget, 2) over (partition by location order by last7_count_is_legal, id) as prev_budget,
        lead(last7_min_is_legal, 1) over (order by is_legal, id) as next_last7_min_is_legal,
        ntile(3) over (partition by dept_name order by last7_min_is_legal desc, id) as last7_min_is_legal_bucket
    from metrics
    where is_legal > 80
),
filtered as (
    select
        *,
        cast(is_legal as double) as is_legal_str,
        round(prev_budget, 2) as prev_budget_rounded,
        coalesce(location, 'DIRECT') as location_clean
    from metrics_departments
    where location is distinct from 'London'
),
with_windows as (
    select
        *,
        round(prev_budget / sum(prev_budget) over (partition by location), 4) as prev_budget_share,
        ntile(4) over w_hist as budget_share_bucket,
        ntile(3) over w_hist_2 as budget_share_bucket_2,
        last_value(next_last7_min_is_legal) ignore nulls over (order by next_last7_min_is_legal, id rows between unbounded preceding and current row) as last_known_next_last7_min_is_legal
    from filtered
    window w_hist as (partition by dept_name order by budget_share desc, id), w_hist_2 as (order by budget_share desc, id)
)
select
    id,
    dept_name,
    location,
    budget,
    location_upper,
    last7_min_is_legal,
    last7_sum_is_legal,
    budget_share,
    prev_budget
from with_windows
order by last7_sum_is_legal;
