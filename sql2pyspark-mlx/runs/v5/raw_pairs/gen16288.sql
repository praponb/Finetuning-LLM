with with_dims as (
    select
        a.id,
        a.store_id,
        a.product_id,
        a.sale_date,
        s.sqft,
        s.opened_date
    from store_sales a
    left join stores s
        on a.store_id = s.id
),
with_windows as (
    select
        *,
        min(sqft) over (partition by store_id, product_id order by opened_date, id rows between 6 preceding and current row) as last7_min_sqft,
        sum(sqft) over (partition by store_id, product_id order by opened_date, id rows between 6 preceding and current row) as last7_sum_sqft,
        count(sqft) over (partition by store_id, product_id order by opened_date, id rows between 6 preceding and current row) as last7_count_sqft,
        max(sqft) over (partition by store_id, product_id) as store_id_max_sqft
    from with_dims
),
with_dims_store_sales as (
    select
        a.id,
        a.store_id,
        a.product_id,
        a.sale_date,
        a.opened_date,
        a.last7_sum_sqft,
        a.store_id_max_sqft,
        p.city,
        p.store_name,
        p.opened_date as store_opened_date
    from with_windows a
    left join stores p
        on a.store_id = p.id
),
latest as (
    select *
    from (
        select *, row_number() over (partition by product_id order by opened_date desc, id) as rn
        from with_dims_store_sales
    ) x
    where rn = 1
)
select
    store_id,
    product_id,
    city,
    sort_array(collect_set(city)) as city_list,
    count(case when city = 'Tokyo' then 1 end) as tokyo_count,
    rank() over (partition by store_id order by count(case when city = 'Tokyo' then 1 end) desc) as tokyo_count_rank
from latest
group by 1, 2, 3;
