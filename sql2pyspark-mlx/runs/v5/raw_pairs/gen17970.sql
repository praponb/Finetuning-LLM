with with_windows as (
    -- Ranking and gap features
    select
        *,
        lag(cost) over (partition by status order by shipped_date, id) as prev_cost,
        lead(cost) over (partition by status order by shipped_date, id) as next_cost,
        row_number() over (partition by status order by shipped_date, id) as seq_no,
        avg(cost) over (partition by status order by delivered_date, id rows between 2 preceding and current row) as cost_avg_2
    from shipments
    where carrier = 'DHL'
),
kept_keys as (
    -- Set intersection on keys
    select status, id
    from with_windows
    intersect all
    select status, id
    from shipments
    where month(shipped_date) in (2, 10, 11)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.status = k.status and s.id = k.id
),
base as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        prev_cost,
        next_cost,
        seq_no,
        substring(status, 1, 2) as status_prefix
    from reconciled
    where carrier is not null
),
kept_keys_2 as (
    -- Set difference on keys
    select id, order_id
    from base
    except
    select id, order_id
    from shipments
    where carrier = 'Kerry'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys_2 k on s.id = k.id and s.order_id = k.order_id
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by status order by cost desc nulls last) as cost_percent_rank,
        min(seq_no) over w_ordered as status_min_seq_no,
        sum(next_cost) over (partition by status, order_id) as status_sum_next_cost
    from kept_rows
    window w_ordered as (partition by status)
)
select
    s.id,
    s.order_id,
    s.carrier,
    s.status,
    s.prev_cost,
    s.next_cost,
    s.seq_no,
    s.status_prefix,
    s.cost_percent_rank,
    case when status rlike '^(DELIVERED|IN_TRANSIT)$' then 'EXTENDED' when status rlike '^(LOST|RETURNED)$' then 'CORE' else 'GROUP_B' end as status_zone,
    (select max(p2.amount) from payments p2 where p2.order_id = s.order_id) as max_payment_amount
from enriched s
