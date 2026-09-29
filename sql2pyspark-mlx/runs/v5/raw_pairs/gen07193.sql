with enriched as (
    -- Attach products attributes
    select
        s.id,
        s.store_id,
        s.product_id,
        s.units,
        s.revenue,
        p.category,
        p.brand,
        p.cost
    from store_sales s
    left join products p
        on s.product_id = p.id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        units,
        revenue,
        category,
        brand,
        cost,
        substring(category, 1, 3) as category_prefix
    from enriched
    where category is not null
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        count(cost) over w_hist as running_count_cost,
        sum(cost) over w_hist as running_sum_cost,
        max(cost) over w_hist as running_max_cost,
        round(cost / sum(cost) over (partition by category, brand), 4) as cost_share
    from cleaned
    window w_hist as (partition by brand, category order by cost, id rows between unbounded preceding and current row)
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by brand order by running_sum_cost desc nulls last, id) as row_num
    from with_windows
    qualify row_num = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        units,
        category,
        cost_share,
        cume_dist() over (partition by category order by running_sum_cost desc nulls last) as running_sum_cost_cume_dist,
        avg(running_sum_cost) over (partition by brand order by cost, id rows between 3 preceding and 1 preceding) as running_sum_cost_avg_3_prior
    from deduped
)
-- Multi-level aggregation
select
    category,
    count(*) filter (where category = 'Home') as home_rows,
    count_if(category = 'Electronics') as electronics_events,
    sort_array(collect_set(category)) as category_list,
    percentile_approx(units, 0.5) as units_p50
from scored
group by category
having count_if(category = 'Electronics') >= 1
