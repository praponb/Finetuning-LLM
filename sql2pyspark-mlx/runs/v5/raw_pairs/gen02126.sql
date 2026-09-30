with ranked as (
    select *, row_number() over (partition by product_id order by last_restock nulls last, id) as latest_rank
    from inventory
    qualify latest_rank = 1
),
enriched as (
    select e.*, d.opened_date
    from ranked e
    inner join stores d
        on e.store_id = d.id
    where d.city = 'Singapore'
),
with_dims as (
    select
        e.*,
        p.brand,
        p.product_name,
        p.category
    from enriched e
    left join products p
        on e.product_id = p.id
),
scored as (
    select
        *,
        avg(quantity) over w_grp as running_avg_quantity,
        sum(quantity) over w_grp as running_sum_quantity,
        count(quantity) over w_grp as running_count_quantity,
        row_number() over w_part as row_seq
    from with_dims
    window w_grp as (partition by product_id order by last_restock, id rows between unbounded preceding and current row), w_part as (partition by store_id order by opened_date, id)
),
enriched_inventory as (
    select
        s.id,
        s.store_id,
        s.product_id,
        s.latest_rank,
        s.product_name,
        s.category,
        s.running_count_quantity,
        s.row_seq,
        b.city,
        b.sqft
    from scored s
    left join stores b
        on s.store_id = b.id
)
select
    case when row_seq >= 6 then 'LARGE' else 'SMALL' end as row_seq_tier,
    case when running_count_quantity >= 17 then 'LARGE' else 'SMALL' end as running_count_quantity_tier,
    approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
    approx_percentile(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles,
    max(running_count_quantity) filter (where category = 'Books') as books_max_running_count_quantity,
    sum(case when category = 'Books' then row_seq else 0 end) as books_row_seq,
    sum(case when category = 'Electronics' then sqft else 0 end) as electronics_sqft
from enriched_inventory
group by case when row_seq >= 6 then 'LARGE' else 'SMALL' end, case when running_count_quantity >= 17 then 'LARGE' else 'SMALL' end
having books_row_seq < 1000000
