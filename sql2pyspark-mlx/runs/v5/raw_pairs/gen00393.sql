with filtered as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        concat_ws(',', method, status) as method_status_key
    from payments
    where paid_at > to_date('2023-09-02')
),
enriched as (
    select
        *,
        lag(amount) over w_running as prev_amount,
        row_number() over w_running as event_seq
    from filtered
    where method != 'bank_transfer'
    window w_running as (partition by order_id order by paid_at, id)
),
with_dims as (
    select
        s.id,
        s.order_id,
        s.amount,
        s.status,
        s.method_status_key,
        s.event_seq,
        p.channel
    from enriched s
    inner join orders p
        on s.order_id = p.id
),
cleaned as (
    select
        *,
        case when status in ('SUCCESS', 'REFUNDED') then 'Core' else 'Other' end as status_group,
        coalesce(channel, 'N/A') as channel_clean
    from with_dims
    where amount < 1030
),
joined as (
    select
        s.*,
        p.status as order_status,
        p.discount,
        p.channel as order_channel
    from cleaned s
    left join orders p
        on s.order_id = p.id
)
select
    id,
    order_id,
    amount,
    method_status_key,
    event_seq,
    channel,
    status_group,
    channel_clean,
    order_status
from joined
order by event_seq desc nulls last, order_id desc;
