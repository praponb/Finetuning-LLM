with base as (
    select
        *,
        datediff(current_date(), sale_date) as days_since_sale_date,
        date_format(sale_date, 'yyyy-MM-dd') as sale_date_label
    from store_sales
    where revenue between 2300 and 3100
),
with_windows as (
    select
        *,
        avg(units) over w_running as units_avg_3,
        percent_rank() over (partition by product_id, store_id order by revenue desc) as revenue_percent_rank,
        ntile(3) over (order by units, id) as units_bucket,
        min(units) over w_grp as product_id_min_units
    from base
    window w_running as (partition by product_id order by days_since_sale_date, id rows between 3 preceding and current row), w_grp as (partition by product_id, store_id)
),
enriched as (
    select
        e.id,
        e.store_id,
        e.revenue_percent_rank,
        e.units_bucket,
        e.product_id_min_units,
        s.city,
        s.region,
        s.sqft
    from with_windows e
    inner join stores s
        on e.store_id = s.id
),
enriched_store_sales as (
    select s.*, d.sqft as store_sqft
    from enriched s
    left join stores d
        on s.store_id = d.id
),
metrics as (
    select
        *,
        lead(product_id_min_units, 1) over w_ordered as next_product_id_min_units,
        min(revenue_percent_rank) over (partition by region) as region_min_revenue_percent_rank,
        sum(product_id_min_units) over (partition by store_id order by units_bucket, id rows between unbounded preceding and current row) as cumulative_product_id_min_units
    from enriched_store_sales
    window w_ordered as (partition by store_id order by product_id_min_units, id)
),
with_windows_store_sales as (
    select
        id,
        store_id,
        revenue_percent_rank,
        units_bucket,
        city,
        region,
        sqft,
        store_sqft,
        next_product_id_min_units,
        avg(next_product_id_min_units) over w_grp as last7_avg_next_product_id_min_units,
        sum(next_product_id_min_units) over w_grp as last7_sum_next_product_id_min_units,
        max(next_product_id_min_units) over w_grp as last7_max_next_product_id_min_units,
        min(next_product_id_min_units) over w_grp as last7_min_next_product_id_min_units,
        ntile(5) over (order by sqft, id) as sqft_bucket
    from metrics
    window w_grp as (partition by region, store_id order by revenue_percent_rank, id rows between 6 preceding and current row)
),
wide as (
    select *
    from (
        select
            store_id,
            city,
            region,
            last7_max_next_product_id_min_units
        from with_windows_store_sales
    ) src
    pivot (
        max(last7_max_next_product_id_min_units) as value_sum,
        count(1) as cnt
        for region in ('Central' as central, 'East' as east, 'South' as south)
    )
)
select
    store_id,
    city,
    central_cnt,
    east_value_sum,
    south_cnt
from wide
order by store_id nulls last;
