with enriched as (
    select
        a.*,
        p.city,
        p.store_name
    from store_sales a
    left join stores p
        on a.store_id = p.id
),
latest as (
    select *
    from (
        select *, row_number() over (partition by city, store_id order by units desc, id) as rn
        from enriched
    ) t
    where rn = 1
),
enriched_store_sales as (
    select
        *,
        round(revenue / sum(revenue) over (partition by store_id, product_id), 4) as revenue_share,
        sum(units) over (partition by city, product_id order by units, id rows between unbounded preceding and current row) as cumulative_units,
        row_number() over (partition by product_id order by sale_date desc, id) as seq_no
    from latest
),
joined as (
    select
        src.*,
        d.category,
        d.stock_qty
    from enriched_store_sales src
    join products d
        on src.product_id = d.id
),
with_dims as (
    select
        e.*,
        s.store_name as store_store_name,
        s.region
    from joined e
    left join stores s
        on e.store_id = s.id
),
with_dims_store_sales as (
    select
        e.*,
        s.city as store_city,
        s.sqft
    from with_dims e
    join stores s
        on e.store_id = s.id
),
joined_store_sales as (
    select s.*, p.stock_qty as product_stock_qty
    from with_dims_store_sales s
    left join products p
        on s.product_id = p.id
)
select *
from (
    select
        id,
        store_city,
        revenue_share,
        revenue
    from joined_store_sales
) src
unpivot (
    metric_value for metric in (revenue_share, revenue)
);
