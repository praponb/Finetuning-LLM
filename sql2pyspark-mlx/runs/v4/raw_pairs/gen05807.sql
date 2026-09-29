with base as (
    select
        *,
        coalesce(quantity, 0) as quantity_filled,
        case when quantity >= 207 then 'High' when quantity >= 48 then 'Medium' else 'Low' end as quantity_band
    from inventory
    where last_restock >= date '2024-04-05'
      and month(last_restock) in (5, 8, 10)
),
with_windows as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_filled,
        quantity_band,
        row_number() over w_hist as event_seq,
        row_number() over w_hist_2 as event_seq_2,
        ntile(3) over (partition by product_id order by quantity_filled desc, id) as quantity_filled_bucket,
        row_number() over (partition by store_id order by quantity_filled, id) as seq_no
    from base
    window w_hist as (partition by product_id order by last_restock desc, id), w_hist_2 as (partition by product_id, store_id order by quantity, id)
),
with_dims as (
    select
        a.*,
        d.stock_qty,
        d.price,
        d.category
    from with_windows a
    inner join products d
        on a.product_id = d.id
),
stats as (
    select
        seq_no,
        bit_or(quantity) as quantity_bit_or,
        count(case when category = 'Fashion' then 1 end) as fashion_count,
        count_if(category = 'Books') as books_events
    from with_dims
    group by seq_no
)
select seq_no, fashion_count
from stats
order by seq_no desc;
