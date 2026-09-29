with with_windows as (
    select
        id,
        store_id,
        max(revenue) over (partition by product_id, store_id order by revenue, id rows between unbounded preceding and current row) as running_max_revenue,
        min(revenue) over (partition by product_id, store_id order by revenue, id rows between unbounded preceding and current row) as running_min_revenue,
        avg(revenue) over (partition by product_id, store_id order by revenue, id rows between unbounded preceding and current row) as running_avg_revenue,
        last_value(revenue) ignore nulls over (partition by product_id order by sale_date, id rows between unbounded preceding and current row) as last_known_revenue
    from store_sales
    where product_id = 6
),
scored as (
    select
        *,
        sum(running_min_revenue) over (order by running_max_revenue, id rows between unbounded preceding and current row) as cumulative_running_min_revenue,
        sum(running_min_revenue) over (partition by store_id order by running_max_revenue, id rows between unbounded preceding and current row) as running_running_min_revenue
    from with_windows
),
base as (
    select
        id,
        running_max_revenue,
        running_avg_revenue,
        last_known_revenue,
        cumulative_running_min_revenue,
        running_running_min_revenue,
        round(cumulative_running_min_revenue / running_avg_revenue, 2) as cumulative_running_min_revenue_per_running_avg_revenue
    from scored
    where running_max_revenue <= 3100
),
base_store_sales as (
    select
        *,
        coalesce(running_running_min_revenue, 0) as running_running_min_revenue_filled,
        running_avg_revenue + cumulative_running_min_revenue_per_running_avg_revenue as running_avg_revenue_plus_cumulative_running_min_revenue_per_running_avg_revenue
    from base
    where running_avg_revenue <= 1200
),
cleaned as (
    select *, greatest(running_running_min_revenue, running_max_revenue) as greatest_running_running_min_revenue_running_max_revenue
    from base_store_sales
    where running_running_min_revenue > 15004
)
select
    case when running_avg_revenue_plus_cumulative_running_min_revenue_per_running_avg_revenue >= 65 then 'LARGE' else 'SMALL' end as running_avg_revenue_plus_cumulative_running_min_revenue_per_running_avg_revenue_tier,
    case when cumulative_running_min_revenue_per_running_avg_revenue >= 84 then 'LARGE' else 'SMALL' end as cumulative_running_min_revenue_per_running_avg_revenue_tier,
    count(*) as event_count,
    max(running_avg_revenue) as max_running_avg_revenue,
    approx_percentile(cumulative_running_min_revenue, array(0.25, 0.5, 0.75)) as cumulative_running_min_revenue_quartiles
from cleaned
group by 1, 2
having max_running_avg_revenue > 0
order by max_running_avg_revenue desc;
