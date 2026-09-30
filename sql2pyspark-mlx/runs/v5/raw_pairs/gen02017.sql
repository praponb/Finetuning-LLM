with cleaned as (
    -- Basic cleansing and defaults
    select *, month(opened_date) as opened_date_month
    from stores
    where opened_date > date '2023-03-04'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date <= s.opened_date
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(opened_date_month) ignore nulls over (partition by city order by sqft asc, id asc rows between unbounded preceding and current row) as last_known_opened_date_month,
        sum(opened_date_month) over (partition by region, city order by opened_date_month asc, id asc rows between unbounded preceding and current row) as cumulative_opened_date_month
    from eligible
    where sqft = 10833
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        avg(cumulative_opened_date_month) over (partition by city order by opened_date asc, id asc rows between 6 preceding and 1 preceding) as cumulative_opened_date_month_avg_6_prior,
        row_number() over (partition by city order by opened_date_month desc, id asc) as row_seq
    from scored
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by city order by row_seq desc nulls last) as row_seq_cume_dist,
        stddev_samp(cumulative_opened_date_month) over (partition by city) as city_stddev_samp_cumulative_opened_date_month,
        ntile(4) over (partition by city order by row_seq desc, id asc) as row_seq_bucket,
        last_value(opened_date_month) ignore nulls over (partition by region order by sqft asc, id asc rows between unbounded preceding and current row) as last_known_opened_date_month_2,
        sum(cumulative_opened_date_month_avg_6_prior) over (partition by city order by last_known_opened_date_month asc, id asc rows between unbounded preceding and current row) as running_cumulative_opened_date_month_avg_6_prior
    from enriched
),
cleaned_stores as (
    -- Keep recent, valid rows
    select
        *,
        case when region = 'West' then 1 else 0 end as is_west,
        coalesce(region, 'DIRECT') as resolved_region
    from metrics
    where city_stddev_samp_cumulative_opened_date_month between 212 and 262
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        region,
        row_seq_bucket
    from cleaned_stores
) src
pivot (
    avg(row_seq_bucket)
    for region in ('East', 'South')
);
