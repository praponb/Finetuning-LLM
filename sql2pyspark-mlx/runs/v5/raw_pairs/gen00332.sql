with eligible as (
    select s.*
    from stores s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock < s.opened_date
        )
),
scored as (
    select
        id,
        city,
        region,
        sqft,
        avg(sqft) over w_recent running_avg_sqft,
        count(sqft) over w_recent running_count_sqft,
        min(sqft) over w_recent running_min_sqft,
        max(sqft) over w_recent running_max_sqft
    from eligible
    window w_recent as (partition by city, region order by sqft, id rows between unbounded preceding and current row)
),
deduped as (
    select
        id,
        region,
        running_avg_sqft,
        running_count_sqft,
        running_min_sqft,
        running_max_sqft
    from (
        select *, row_number() over (partition by region, city order by running_count_sqft desc nulls last, id) as row_num
        from scored
    ) t
    where row_num = 1
),
with_windows as (
    select
        id,
        running_avg_sqft,
        sum(running_max_sqft) over (partition by region) region_sum_running_max_sqft,
        avg(running_avg_sqft) over (partition by region order by running_max_sqft, id rows between 5 preceding and current row) running_avg_sqft_avg_5
    from deduped
)
select
    case when running_avg_sqft_avg_5 >= 12789 then 'LARGE' else 'SMALL' end as running_avg_sqft_avg_5_tier,
    min(region_sum_running_max_sqft) as min_region_sum_running_max_sqft,
    approx_percentile(running_avg_sqft_avg_5, 0.9) as running_avg_sqft_avg_5_p90,
    approx_percentile(region_sum_running_max_sqft, array(0.25, 0.5, 0.75)) as region_sum_running_max_sqft_quartiles
from with_windows
group by running_avg_sqft_avg_5_tier
order by running_avg_sqft_avg_5_p90 desc nulls last;
