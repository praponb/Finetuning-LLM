with eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from stores s
    where exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.sale_date >= s.opened_date
        )
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from eligible
    qualify rank() over (partition by region, city order by sqft desc) = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, lower(city) as city_lower
    from ranked
    where opened_date > date '2024-02-19'
      and region is not null
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date <= s.opened_date
        )
),
valid_stores as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        add_months(opened_date, 12) as opened_date_plus_months,
        coalesce(region, 'NONE') as region_filled
    from not_flagged
    where opened_date >= date '2023-11-16'
      and region is not null
      and store_name is null
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, top1.sale_date
    from valid_stores s
    join lateral (
            select c.sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    -- Running and moving aggregates
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        city_lower,
        opened_date_plus_months,
        region_filled,
        sale_date,
        avg(sqft) over (order by opened_date, id rows between 5 preceding and current row) as sqft_avg_5,
        lag(sqft, 1) over w_ordered as prev_sqft
    from with_last
    window w_ordered as (order by sale_date, id)
)
-- Running and moving aggregates
select
    *,
    row_number() over (partition by region order by sale_date desc, id) as event_seq,
    row_number() over (partition by region, city order by opened_date_plus_months, id) as seq_no,
    lag(sqft, 2) over (partition by region order by sale_date, id) as prev_sqft_2
from scored;
