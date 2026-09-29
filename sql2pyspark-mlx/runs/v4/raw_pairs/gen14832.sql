with filtered_departments as (
    select *
    from departments
    where location = 'New York'
),
budget_bucket_summary as (
    select case when budget >= 3516000 then 'high' when budget >= 788000 then 'mid' else 'low' end as budget_bucket, concat_ws(', ', sort_array(collect_set(location))) as location_values
    from filtered_departments
    group by case when budget >= 3516000 then 'high' when budget >= 788000 then 'mid' else 'low' end
)
select budget_bucket, location_values
from budget_bucket_summary
order by location_values;
