with scored as (
    -- Ranking and gap features
    select
        id,
        carrier,
        delivered_date,
        cost,
        lead(cost) over (partition by carrier, order_id order by shipped_date, id) as next_cost,
        lag(cost) over (partition by carrier, order_id order by shipped_date, id) as prev_cost,
        row_number() over (partition by carrier, order_id order by shipped_date, id) as row_seq,
        last_value(cost, true) over (order by shipped_date, id rows between unbounded preceding and current row) as last_known_cost
    from shipments
    where shipped_date > '2025-10-01'
)
select
    id,
    delivered_date,
    cost,
    next_cost,
    prev_cost,
    last_known_cost
from scored
order by next_cost desc, cost;
