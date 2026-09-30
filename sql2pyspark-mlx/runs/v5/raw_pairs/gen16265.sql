with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        country,
        coalesce(supplier_name, 'unassigned') as supplier_name_filled
    from suppliers
    where city is not null
      and city is not distinct from 'New York'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and array_contains(c.tags, 'sale')
        )
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name_filled,
        latest.launch_date,
        latest.product_cost
    from kept s
    left join lateral (
            select c.launch_date, c.cost as product_cost
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) latest on true
),
kept_keys as (
    -- Set difference on keys
    select id
    from with_latest
    except
    select id
    from suppliers
    where supplier_name like 'Sia%'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_latest s
    inner join kept_keys k on s.id = k.id
),
base as (
    -- Keep recent, valid rows
    select *, date_sub(launch_date, 52) as launch_date_minus_52d
    from kept_rows
    where product_cost <= 390
),
kept_suppliers as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.launch_date < s.launch_date_minus_52d
        )
)
-- Aggregate with conditional metrics
select
    date_trunc('week', launch_date_minus_52d) as week_start,
    max(product_cost) as max_product_cost,
    approx_percentile(product_cost, 0.5) as product_cost_p50,
    count(*) as cnt,
    avg(product_cost) as avg_product_cost,
    count(*) as event_count,
    count(*) as cnt_2
from kept_suppliers
group by rollup (date_trunc('week', launch_date_minus_52d))
having max_product_cost < 1000000;
