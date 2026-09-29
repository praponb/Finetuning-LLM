with remaining_keys as (
    -- Set difference on keys
    select carrier
    from shipments
    except all
    select carrier
    from shipments
    where cost >= 56
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from shipments s
    inner join remaining_keys k on s.carrier = k.carrier
),
enriched as (
    -- Ranking and gap features
    select
        *,
        stddev_samp(cost) over w_grp as order_id_stddev_samp_cost,
        round(cost / sum(cost) over (partition by status), 4) as cost_share
    from reconciled
    window w_grp as (partition by order_id, status)
),
enriched_shipments as (
    -- Attach orders attributes
    select
        a.*,
        o.channel,
        o.status as order_status
    from enriched a
    left join orders o
        on a.order_id = o.id
    where o.amount > 610
),
enriched_shipments_2 as (
    -- Attach orders attributes
    select
        a.*,
        p.order_date,
        p.discount
    from enriched_shipments a
    left join orders p
        on a.order_id = p.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        order_id,
        shipped_date,
        delivered_date,
        cost_share,
        order_date,
        percent_rank() over (partition by order_id, status order by order_id_stddev_samp_cost desc) as order_id_stddev_samp_cost_percent_rank,
        round(cost / sum(cost) over (partition by carrier, order_status), 4) as cost_share_2,
        count(discount) over w_part as order_status_count_discount
    from enriched_shipments_2
    window w_part as (partition by order_status, status)
)
select
    shipped_date,
    delivered_date,
    cost_share,
    order_id_stddev_samp_cost_percent_rank,
    cost_share_2
from metrics
order by cost_share_2 asc
