with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        supplier_name,
        city,
        regexp_replace(supplier_name, ' ', '_') as supplier_name_slug,
        coalesce(city, 'OTHER') as resolved_city
    from suppliers
    where city is not null
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.supplier_name_slug,
        top1.launch_date,
        top1.category
    from base s
    left join lateral (
            select c.launch_date, c.category
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) top1 on true
),
enriched as (
    -- Window metrics over several different partitions
    select *, last_value(city) ignore nulls over (order by launch_date, id rows between unbounded preceding and current row) as last_known_city
    from with_last
    where month(launch_date) in (1, 3, 9)
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, top1.product_category
    from enriched s
    join lateral (
            select c.category as product_category
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) top1 on true
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', launch_date) as quarter_start,
    count_if(product_category = 'Home') as home_events,
    count(*) as event_count,
    sort_array(collect_set(city)) as city_list,
    count(*) as event_count_2,
    count_if(product_category = 'Grocery') as grocery_events
from with_latest
group by 1
having grocery_events > 1
order by grocery_events desc;
