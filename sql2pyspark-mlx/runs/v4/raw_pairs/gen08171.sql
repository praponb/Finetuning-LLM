with cleaned as (
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        add_months(shipped_date, 3) as shipped_date_plus_months
    from shipments
    where order_id is not null
      and order_id = 21
),
enriched as (
    select
        a.*,
        o.channel,
        o.amount
    from cleaned a
    left join orders o
        on a.order_id = o.id
),
ranked as (
    select *
    from enriched
    qualify row_number() over (partition by carrier order by delivered_date desc, id) = 1
)
select
    date_trunc('month', shipped_date_plus_months) as activity_month,
    carrier,
    channel,
    count(*) filter (where carrier = 'DHL') as dhl_rows,
    count(case when channel = 'web' then 1 end) as web_count
from ranked
group by rollup (date_trunc('month', shipped_date_plus_months), carrier, channel)
order by web_count
