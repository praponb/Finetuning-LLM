with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        units,
        last_day(sale_date) sale_date_month_end
    from store_sales
    where sale_date >= date '2023-06-20'
),
joined as (
    -- Attach stores attributes
    select s.*, d.city
    from filtered s
    left join stores d
        on s.store_id = d.id
    where d.opened_date >= '2024-12-07'
),
joined_store_sales as (
    -- Attach stores attributes
    select
        a.*,
        s.store_name,
        s.sqft,
        s.city as store_city
    from joined a
    left join stores s
        on a.store_id = s.id
),
enriched as (
    -- Attach stores attributes
    select
        a.*,
        p.opened_date,
        p.store_name as store_store_name
    from joined_store_sales a
    left join stores p
        on a.store_id = p.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        units,
        sqft,
        store_store_name,
        count(sqft) over w_grp store_id_count_sqft,
        avg(units) over (partition by store_city order by opened_date asc, id asc rows between 5 preceding and current row) units_avg_5,
        dense_rank() over (partition by city order by units asc nulls last) units_dense_rank,
        row_number() over (partition by store_id, store_city order by sale_date_month_end asc, id asc) seq_no,
        first_value(sqft, true) over w_hist first_known_sqft
    from enriched
    window w_grp as (partition by store_id), w_hist as (partition by store_id order by opened_date asc, id asc rows between unbounded preceding and current row)
),
joined_store_sales_2 as (
    -- Attach stores attributes
    select
        a.id,
        a.store_id,
        a.store_store_name,
        a.units_avg_5,
        a.units_dense_rank,
        d.opened_date
    from metrics a
    inner join stores d
        on a.store_id = d.id
),
ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        store_id,
        store_store_name,
        units_avg_5,
        units_dense_rank
    from joined_store_sales_2
    qualify row_number() over (partition by store_id order by units_dense_rank desc, id asc) = 1
)
select
    s.id,
    s.store_id,
    s.store_store_name,
    s.units_avg_5,
    s.units_dense_rank,
    (select min(x.quantity) from inventory x where x.store_id = s.store_id) as min_inventory_quantity
from ranked s
order by min_inventory_quantity desc nulls first
