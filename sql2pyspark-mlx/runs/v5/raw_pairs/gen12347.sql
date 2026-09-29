with filtered as (
    -- Keep recent, valid rows
    select *, floor(cost) as cost_whole
    from shipments
    where shipped_date > date '2023-09-15'
      and carrier is distinct from 'UPS'
),
with_dims as (
    -- Attach orders attributes
    select src.*, b.channel
    from filtered src
    left join orders b
        on src.order_id = b.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(cost) over w_ordered as running_avg_cost,
        count(cost) over w_ordered as running_count_cost,
        max(cost) over w_ordered as running_max_cost
    from with_dims
    where order_id in (15, 25, 26, 31)
    window w_ordered as (partition by carrier order by delivered_date asc, id asc rows between unbounded preceding and current row)
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        channel,
        running_count_cost
    from metrics
    qualify row_number() over (partition by channel order by running_count_cost asc nulls first, id asc) = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lag(running_count_cost) over w_recent as prev_running_count_cost,
        lead(running_count_cost) over w_recent as next_running_count_cost,
        row_number() over w_recent as seq_no
    from deduped
    window w_recent as (partition by channel, carrier order by shipped_date asc, id asc)
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by channel, carrier order by running_count_cost desc nulls first, id asc) as rank_in_group
    from with_windows
    qualify rank_in_group = 1
),
filtered_shipments as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        shipped_date,
        channel,
        next_running_count_cost,
        seq_no,
        datediff(current_date(), shipped_date) as days_since_shipped_date,
        coalesce(carrier, 'N/A') as carrier_clean
    from ranked
    where order_id is not null
      and channel in ('store', 'web')
)
select
    s.id,
    s.order_id,
    s.shipped_date,
    s.channel,
    s.next_running_count_cost,
    s.seq_no,
    s.days_since_shipped_date,
    s.carrier_clean,
    (select min(x.amount) from payments x where x.order_id = s.order_id) as min_payment_amount
from filtered_shipments s
order by id asc, next_running_count_cost desc
limit 5
