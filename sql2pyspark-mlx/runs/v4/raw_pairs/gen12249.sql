with filtered as (
    select
        *,
        coalesce(location, 'N/A') as location_filled,
        case when location = 'Singapore' then 1 else 0 end as is_singapore,
        coalesce(dept_name, 'DIRECT') as resolved_dept_name
    from departments
    where location not in ('Bangkok', 'Chiang Mai')
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by dept_name order by is_singapore nulls last, id) as row_num
        from filtered
    ) t
    where row_num = 1
),
with_windows as (
    select
        *,
        sum(budget) over (partition by location order by budget, id rows between unbounded preceding and current row) as cumulative_budget,
        sum(budget) over (partition by location) as location_sum_budget
    from ranked
),
metrics as (
    select
        *,
        last_value(cumulative_budget, true) over (order by budget, id rows between unbounded preceding and current row) as last_known_cumulative_budget,
        sum(budget) over (partition by dept_name order by cumulative_budget, id rows between unbounded preceding and current row) as running_budget,
        sum(location_sum_budget) over (partition by location order by budget, id rows between unbounded preceding and current row) as running_location_sum_budget
    from with_windows
),
enriched as (
    select
        *,
        sum(last_known_cumulative_budget) over w_running as cumulative_last_known_cumulative_budget,
        percent_rank() over w_hist as location_sum_budget_percent_rank
    from metrics
    window w_running as (order by cumulative_budget, id rows between unbounded preceding and current row), w_hist as (partition by dept_name order by location_sum_budget desc)
),
enriched_departments as (
    select
        id,
        dept_name,
        location,
        budget,
        location_filled,
        is_singapore,
        resolved_dept_name,
        row_num,
        cumulative_budget,
        location_sum_budget,
        last_known_cumulative_budget,
        running_budget,
        running_location_sum_budget,
        cumulative_last_known_cumulative_budget,
        location_sum_budget_percent_rank,
        avg(location_sum_budget_percent_rank) over (partition by location order by last_known_cumulative_budget, id rows between 5 preceding and 1 preceding) as location_sum_budget_percent_rank_avg_5_prior,
        last_value(location) ignore nulls over (order by cumulative_last_known_cumulative_budget, id rows between unbounded preceding and current row) as last_known_location,
        dense_rank() over (partition by dept_name order by budget desc) as budget_dense_rank,
        sum(running_location_sum_budget) over (partition by dept_name) as dept_name_sum_running_location_sum_budget
    from enriched
),
eligible as (
    select s.*
    from enriched_departments s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.job_title in ('Engineer', 'Designer', 'Manager')
        )
),
summary as (
    select
        dept_name,
        location,
        case when location_sum_budget_percent_rank >= 0.81 then 'PREMIUM' else 'STANDARD' end as location_sum_budget_percent_rank_tier,
        max(running_budget) filter (where location = 'New York') as new_york_max_running_budget,
        approx_percentile(location_sum_budget_percent_rank_avg_5_prior, 0.5) as location_sum_budget_percent_rank_avg_5_prior_p50,
        count(*) as num_rows
    from eligible
    group by dept_name, location, location_sum_budget_percent_rank_tier
)
select
    dept_name,
    location,
    location_sum_budget_percent_rank_tier,
    location_sum_budget_percent_rank_avg_5_prior_p50
from summary
order by location desc nulls first;
