with enriched as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        lag(quantity) over (partition by store_id order by quantity, id) as prev_quantity,
        row_number() over (partition by store_id order by quantity, id) as row_seq,
        row_number() over (partition by store_id, product_id order by last_restock desc, id) as seq_no
    from inventory
),
enriched_inventory as (
    select
        a.*,
        p.launch_date,
        p.cost
    from enriched a
    left join products p
        on a.product_id = p.id
),
enriched_inventory_2 as (
    select
        e.id,
        e.store_id,
        e.last_restock,
        e.seq_no,
        e.launch_date,
        e.cost,
        s.sqft,
        s.city,
        s.store_name
    from enriched_inventory e
    left join stores s
        on e.store_id = s.id
),
with_windows as (
    select
        id,
        launch_date,
        sqft,
        store_name,
        lag(seq_no) over (partition by store_id order by launch_date, id) as prev_seq_no,
        lead(seq_no) over (partition by store_id order by launch_date, id) as next_seq_no,
        lead(sqft) over (partition by store_id order by launch_date, id) as next_sqft,
        lag(sqft) over (partition by store_id order by launch_date, id) as prev_sqft,
        row_number() over (partition by store_id order by launch_date, id) as seq_no_2,
        ntile(5) over (partition by store_id order by seq_no desc, id) as seq_no_bucket
    from enriched_inventory_2
)
select
    case when next_sqft >= 15576 then 'HIGH' else 'LOW' end as next_sqft_tier,
    prev_seq_no,
    approx_percentile(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles,
    round(avg(sqft), 2) as avg_sqft,
    round(avg(next_seq_no), 2) as avg_next_seq_no
from with_windows
group by 1, 2
having round(avg(sqft), 2) >= 0
