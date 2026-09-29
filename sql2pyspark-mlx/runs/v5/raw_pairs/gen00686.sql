with with_last as (
    select
        s.*,
        latest.store_sale_revenue,
        latest.store_sale_units
    from products s
    left join lateral (
            select c.revenue as store_sale_revenue, c.units as store_sale_units
            from store_sales c
            where c.product_id = s.id
            order by c.sale_date desc, c.id desc
            limit 1
        ) latest on true
),
latest_match as (
    select
        s.id,
        s.category,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.store_sale_revenue,
        s.store_sale_units,
        x.store_sale_sale_date,
        x.revenue
    from with_last s
    left join lateral (
            select c.sale_date as store_sale_sale_date, c.revenue
            from store_sales c
            where c.product_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    select
        *,
        lead(store_sale_revenue) over w_grp as next_store_sale_revenue,
        lag(store_sale_units) over w_grp as prev_store_sale_units,
        row_number() over w_grp as event_seq,
        lead(revenue, 2) over (partition by category, supplier_id order by launch_date, id) as next_revenue
    from latest_match
    window w_grp as (partition by category, supplier_id order by store_sale_units, id)
),
base as (
    select
        id,
        category,
        launch_date,
        tags,
        supplier_id,
        store_sale_revenue,
        store_sale_sale_date,
        revenue,
        prev_store_sale_units,
        event_seq,
        next_revenue,
        greatest(prev_store_sale_units, event_seq) as greatest_prev_store_sale_units_event_seq
    from enriched
    where supplier_id is not null
)
select
    date_trunc('year', launch_date) as year_start,
    percentile_approx(next_revenue, array(0.25, 0.5, 0.75)) as next_revenue_quartiles,
    approx_percentile(store_sale_revenue, 0.5) as store_sale_revenue_p50,
    sum(case when category = 'Grocery' then greatest_prev_store_sale_units_event_seq else 0 end) as grocery_greatest_prev_store_sale_units_event_seq,
    percentile_approx(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles,
    grouping_id() as gid
from base
group by rollup (date_trunc('year', launch_date));
