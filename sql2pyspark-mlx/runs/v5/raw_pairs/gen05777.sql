with scored as (
    select
        *,
        lead(cost) over w_part as next_cost,
        lag(cost) over w_part as prev_cost,
        lag(price) over w_part as prev_price,
        lead(price) over w_part as next_price,
        row_number() over w_part as row_seq,
        avg(price) over w_ordered as price_avg_3_prior
    from products
    where cost between 270 and 300
    window w_part as (partition by supplier_id, brand order by cost, id), w_ordered as (partition by supplier_id, brand order by stock_qty, id rows between 3 preceding and 1 preceding)
)
select
    supplier_id,
    case when row_seq >= 7 then 'LARGE' else 'SMALL' end as row_seq_tier,
    count_if(brand = 'Acme') as acme_events,
    bit_and(stock_qty) as stock_qty_bit_and,
    count(*) filter (where category = 'Grocery') as grocery_rows,
    approx_percentile(next_price, array(0.25, 0.5, 0.75)) as next_price_quartiles,
    approx_percentile(prev_price, array(0.25, 0.5, 0.75)) as prev_price_quartiles
from scored
group by 1, 2
