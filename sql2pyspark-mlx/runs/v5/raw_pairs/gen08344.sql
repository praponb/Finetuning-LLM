with joined as (
    -- Attach orders attributes
    select
        s.*,
        o.discount,
        o.status as order_status,
        o.amount
    from shipments s
    left join orders o
        on s.order_id <=> o.id
    where o.shipping_fee between 11 and 31
),
scored as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over w_hist cost_cume_dist,
        row_number() over (partition by carrier order by amount asc, id asc) seq_no,
        last_value(discount, true) over w_running last_known_discount,
        lag(discount, 1) over (order by discount asc, id asc) prev_discount
    from joined
    window w_hist as (order by cost desc), w_running as (partition by status order by amount asc, id asc rows between unbounded preceding and current row)
),
joined_shipments as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.carrier,
        s.shipped_date,
        s.delivered_date,
        s.cost,
        s.status,
        s.discount,
        s.amount,
        s.cost_cume_dist,
        s.seq_no,
        s.prev_discount,
        d.channel,
        d.status as order_status_2
    from scored s
    join orders d
        on s.order_id = d.id
),
base as (
    -- Keep recent, valid rows
    select
        *,
        seq_no + discount seq_no_plus_discount,
        discount - prev_discount discount_minus_prev_discount,
        coalesce(order_status_2, 'DIRECT') order_status_2_filled
    from joined_shipments
    where shipped_date > date '2023-05-18'
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        status,
        seq_no_plus_discount,
        prev_discount,
        discount
    from base
) src
unpivot (
    val for kpi in (seq_no_plus_discount, prev_discount, discount)
);
