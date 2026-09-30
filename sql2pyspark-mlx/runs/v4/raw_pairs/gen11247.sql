with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from suppliers
    qualify row_number() over (partition by city order by rating desc nulls first, id) = 1
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by country order by rating desc, id) as rn
        from ranked
    ) ranked
    where rn = 1
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(supplier_name, ' - ', country) as supplier_name_country,
        concat(country, ' ', city) as country_city,
        coalesce(city, 'N/A') as city_clean
    from deduped
    where rn = 1
),
latest as (
    -- QUALIFY-style window filter
    select *
    from cleaned
    qualify row_number() over (partition by country order by rating desc nulls first, id) = 1
),
latest_suppliers as (
    -- QUALIFY-style window filter
    select *
    from latest
    qualify row_number() over (partition by country order by rating desc nulls first, id) = 1
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.launch_date,
        top1.stock_qty
    from latest_suppliers s
    left join lateral (
            select c.launch_date, c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) top1 on true
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        rn,
        supplier_name_country,
        country_city,
        city_clean,
        launch_date,
        stock_qty,
        sum(rating) over (partition by city order by rating, id rows between unbounded preceding and current row) as cumulative_rating,
        lead(rating, 1) over (partition by city order by stock_qty, id) as next_rating,
        row_number() over (partition by city order by rn, id) as event_seq,
        lag(stock_qty, 1) over (partition by country order by launch_date, id) as prev_stock_qty,
        ntile(3) over (partition by city order by rating, id) as rating_bucket
    from latest_match
)
-- Native SQL PIVOT
select *
from (
    select
        country,
        city,
        stock_qty
    from with_windows
) src
pivot (
    avg(stock_qty) as value_sum,
    count(1) as item_count
    for city in ('Singapore' as singapore, 'New York' as new_york)
);
