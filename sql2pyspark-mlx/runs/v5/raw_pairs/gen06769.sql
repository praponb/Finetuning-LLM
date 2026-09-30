with deduped as (
    -- QUALIFY-style window filter
    select
        id,
        product_id,
        sale_date,
        units
    from store_sales
    qualify row_number() over (partition by store_id, product_id order by sale_date desc nulls first, id) = 1
),
base as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), sale_date) as days_since_sale_date,
        floor(units) as units_whole
    from deduped
    where sale_date >= date '2023-02-27'
),
joined as (
    -- Attach products attributes
    select
        src.*,
        b.launch_date,
        b.cost,
        b.category
    from base src
    left join products b
        on src.product_id = b.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lead(days_since_sale_date) over w_running as next_days_since_sale_date,
        lag(days_since_sale_date) over w_running as prev_days_since_sale_date,
        lead(cost) over w_running as next_cost,
        row_number() over w_running as event_seq
    from joined
    window w_running as (partition by category order by days_since_sale_date, id)
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by category order by next_days_since_sale_date desc nulls last, id) as row_num
    from metrics
    qualify row_num = 1
)
-- Grouped metrics
select
    date_trunc('week', sale_date) as week_start,
    date_trunc('month', launch_date) as activity_month,
    round(sum(units_whole) / nullif(count(case when category = 'Books' then 1 end), 0), 2) as units_whole_per_books,
    count_if(category = 'Home') as home_events
from ranked
group by date_trunc('week', sale_date), date_trunc('month', launch_date)
order by week_start nulls first;
