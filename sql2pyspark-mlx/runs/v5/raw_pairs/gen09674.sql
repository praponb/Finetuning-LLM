with deduped as (
    select *
    from (
        select *, row_number() over (partition by dept_name, location order by budget desc, id) as latest_rank
        from departments
    ) ranked
    where latest_rank = 1
),
cleaned as (
    select *, latest_rank + budget as latest_rank_plus_budget
    from deduped
    where location is not null
      and latest_rank > 1
),
enriched as (
    select
        id,
        location,
        latest_rank,
        max(latest_rank_plus_budget) over w_ordered as running_max_latest_rank_plus_budget,
        sum(latest_rank_plus_budget) over w_ordered as running_sum_latest_rank_plus_budget,
        avg(latest_rank_plus_budget) over w_ordered as running_avg_latest_rank_plus_budget,
        ntile(4) over (order by budget, id) as budget_bucket
    from cleaned
    window w_ordered as (partition by dept_name order by budget, id rows between unbounded preceding and current row)
),
scored as (
    select
        *,
        sum(running_sum_latest_rank_plus_budget) over (partition by location order by running_sum_latest_rank_plus_budget, id rows between unbounded preceding and current row) as cumulative_running_sum_latest_rank_plus_budget,
        sum(budget_bucket) over (order by running_sum_latest_rank_plus_budget, id rows between unbounded preceding and current row) as running_budget_bucket
    from enriched
),
base as (
    select
        id,
        location,
        running_max_latest_rank_plus_budget,
        running_avg_latest_rank_plus_budget,
        cumulative_running_sum_latest_rank_plus_budget,
        running_budget_bucket,
        round(cumulative_running_sum_latest_rank_plus_budget, 1) as cumulative_running_sum_latest_rank_plus_budget_rounded,
        coalesce(location, 'OTHER') as location_filled
    from scored
    where cumulative_running_sum_latest_rank_plus_budget < 1639
),
kept as (
    select s.*
    from base s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.dept_id in (14, 15, 31)
        )
)
select
    location,
    case when cumulative_running_sum_latest_rank_plus_budget >= 1859 then 'HIGH' else 'LOW' end as cumulative_running_sum_latest_rank_plus_budget_tier,
    size(collect_set(location)) as num_locations,
    avg(running_avg_latest_rank_plus_budget) as avg_running_avg_latest_rank_plus_budget,
    approx_percentile(running_max_latest_rank_plus_budget, 0.95) as running_max_latest_rank_plus_budget_p95,
    dense_rank() over (partition by location order by avg(running_avg_latest_rank_plus_budget) desc) as avg_running_avg_latest_rank_plus_budget_dense_rank
from kept
group by location, case when cumulative_running_sum_latest_rank_plus_budget >= 1859 then 'HIGH' else 'LOW' end;
