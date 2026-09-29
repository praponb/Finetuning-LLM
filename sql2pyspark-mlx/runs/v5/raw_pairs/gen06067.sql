with valid as (
    select *, concat_ws('-', supplier_name, country) supplier_name_country_key
    from suppliers
    where city is not distinct from 'Tokyo'
),
latest_match as (
    select
        s.id,
        s.supplier_name,
        s.supplier_name_country_key,
        top1.product_stock_qty
    from valid s
    left join lateral (
            select c.stock_qty as product_stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from latest_match s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.stock_qty < 144
        )
),
with_windows as (
    select
        *,
        lead(product_stock_qty, 1) over w_hist next_product_stock_qty,
        row_number() over (order by product_stock_qty desc, id) seq_no,
        row_number() over w_hist row_seq,
        cume_dist() over (order by product_stock_qty desc nulls last) product_stock_qty_cume_dist,
        avg(product_stock_qty) over (order by product_stock_qty, id rows between 5 preceding and 1 preceding) product_stock_qty_avg_5_prior
    from not_flagged
    window w_hist as (order by product_stock_qty, id)
)
select
    seq_no,
    case when next_product_stock_qty >= 331 then 'PREMIUM' else 'STANDARD' end as next_product_stock_qty_tier,
    case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    sum(product_stock_qty_avg_5_prior) as total_product_stock_qty_avg_5_prior,
    min(product_stock_qty) as min_product_stock_qty,
    count(distinct id) as distinct_ids
from with_windows
group by seq_no, case when next_product_stock_qty >= 331 then 'PREMIUM' else 'STANDARD' end, case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end
order by distinct_ids desc
