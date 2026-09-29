with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(status) as status_upper,
        coalesce(carrier, 'OTHER') as carrier_clean
    from shipments
    where carrier is not null
),
scored as (
    -- Ranking and gap features
    select
        *,
        first_value(cost, true) over (order by shipped_date, id rows between unbounded preceding and current row) as first_known_cost,
        sum(cost) over w_ordered as carrier_sum_cost,
        avg(cost) over (partition by status order by shipped_date, id rows between 2 preceding and 1 preceding) as cost_avg_2_prior
    from filtered
    window w_ordered as (partition by carrier)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_upper,
        first_known_cost,
        carrier_sum_cost,
        cost_avg_2_prior,
        carrier_sum_cost * cost as carrier_sum_cost_times_cost
    from scored
    where delivered_date > date '2023-08-17'
      and carrier is not null
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by order_id, status order by carrier_sum_cost desc, id) as rank_in_group
    from valid
    qualify rank_in_group = 1
),
scored_shipments as (
    -- Ranking and gap features
    select
        *,
        max(cost_avg_2_prior) over (partition by order_id) as order_id_max_cost_avg_2_prior,
        stddev_samp(cost) over (partition by carrier) as carrier_stddev_samp_cost
    from ranked
),
with_dims as (
    -- Attach orders attributes
    select s.*, o.status as order_status
    from scored_shipments s
    left join orders o
        on s.order_id = o.id
)
-- Native SQL PIVOT
select *
from (
    select
        status,
        order_status,
        carrier,
        cost
    from with_dims
) src
pivot (
    sum(cost)
    for carrier in ('Flash', 'FedEx', 'UPS')
);
