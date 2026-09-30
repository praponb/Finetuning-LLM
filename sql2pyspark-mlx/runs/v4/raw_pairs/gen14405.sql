with valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        datediff(current_date(), delivered_date) as days_since_delivered_date
    from shipments
    where shipped_date > date '2023-09-29'
      and order_id is not null
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        d.status,
        d.channel
    from valid a
    inner join orders d
        on a.order_id = d.id
),
attributed as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from with_dims s
    inner join promotions pr
        on coalesce(s.channel, 'store') = pr.channel
       and s.shipped_date >= pr.starts_on
       and s.shipped_date < pr.ends_on + interval 14 days
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by promo_code order by cost, id, promo_id) as latest_rank
    from attributed
    qualify latest_rank = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        round(promo_id / sum(promo_id) over (partition by status), 4) as promo_id_share,
        row_number() over (partition by channel, promo_code order by discount_pct desc, id, promo_id) as seq_no,
        row_number() over (partition by channel order by promo_id, id, promo_id) as row_seq,
        row_number() over (partition by status order by shipped_date, id, promo_id) as row_seq_2
    from deduped
),
valid_shipments as (
    -- Basic cleansing and defaults
    select
        id,
        carrier,
        shipped_date,
        cost,
        days_since_delivered_date,
        status,
        channel,
        promo_id,
        promo_code,
        discount_pct,
        latest_rank,
        promo_id_share,
        seq_no,
        row_seq,
        row_seq_2,
        promo_id_share * seq_no as promo_id_share_times_seq_no,
        coalesce(row_seq, 0) as row_seq_filled,
        coalesce(status, 'N/A') as resolved_status
    from with_windows
    where shipped_date > date '2023-04-20'
      and promo_code is not null
)
-- Multi-level aggregation
select
    date_trunc('month', shipped_date) as activity_month,
    date_trunc('week', shipped_date) as week_start,
    count(case when status = 'COMPLETED' then 1 end) as completed_count,
    sort_array(collect_set(promo_code)) as promo_code_list,
    sum(row_seq_2) filter (where promo_code = 'BUNDLE') as bundle_total_row_seq_2,
    sort_array(collect_set(carrier)) as carrier_list,
    size(collect_set(channel)) as num_channels,
    round(avg(cost), 2) as avg_cost
from valid_shipments
group by cube (date_trunc('month', shipped_date), date_trunc('week', shipped_date))
order by activity_month desc nulls last
