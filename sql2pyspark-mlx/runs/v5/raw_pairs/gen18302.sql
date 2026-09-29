with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        quantity,
        last_restock,
        date_format(last_restock, 'yyyy-MM-dd') as last_restock_label,
        year(last_restock) as last_restock_year
    from inventory
    where product_id = 9
),
kept_keys as (
    -- Set difference on keys
    select store_id, id
    from cleaned
    except distinct
    select store_id, id
    from inventory
    where quantity < 228
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.store_id = k.store_id and s.id = k.id
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_id,
        last_restock,
        last_restock_year,
        stddev_samp(quantity) over (partition by store_id) as store_id_stddev_samp_quantity,
        stddev_samp(last_restock_year) over (partition by store_id) as store_id_stddev_samp_last_restock_year,
        max(quantity) over (partition by store_id) as store_id_max_quantity,
        percent_rank() over (order by quantity) as quantity_percent_rank
    from kept_rows
    where quantity between 118 and 195
),
joined as (
    -- Attach stores attributes
    select
        e.store_id,
        e.last_restock_year,
        e.store_id_stddev_samp_quantity,
        e.store_id_max_quantity,
        s.city,
        s.region,
        s.store_name
    from enriched e
    left join stores s
        on e.store_id = s.id
)
-- Multi-level aggregation
select
    store_id,
    count(*) filter (where city = 'London') as london_rows,
    max(last_restock_year) filter (where region = 'North') as north_max_last_restock_year,
    approx_percentile(store_id_stddev_samp_quantity, array(0.25, 0.5, 0.75)) as store_id_stddev_samp_quantity_quartiles,
    round(sum(store_id_max_quantity) / nullif(count(case when city = 'Berlin' then 1 end), 0), 2) as store_id_max_quantity_per_berlin,
    percentile_approx(store_id_stddev_samp_quantity, array(0.25, 0.5, 0.75)) as store_id_stddev_samp_quantity_quartiles_2,
    count(*) as event_count
from joined
group by store_id
order by store_id_max_quantity_per_berlin desc nulls last;
