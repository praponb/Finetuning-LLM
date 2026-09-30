with joined as (
    -- Attach orders attributes
    select
        s.*,
        p.channel,
        p.amount as order_amount,
        p.shipping_fee
    from payments s
    left join orders p
        on s.order_id = p.id
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        channel,
        order_amount,
        shipping_fee,
        row_number() over (partition by status, order_id order by amount desc, id asc) as row_seq,
        round(amount / sum(amount) over (partition by method, order_id), 4) as amount_share
    from joined
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        method,
        amount,
        status,
        channel,
        shipping_fee,
        row_seq,
        case when amount >= 1790 then 'High' when amount >= 750 then 'Medium' else 'Low' end as amount_level,
        amount_share - order_amount as amount_share_minus_order_amount
    from with_windows
    where paid_at >= date '2023-09-24'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from cleaned
    qualify rank() over (partition by status, channel order by shipping_fee desc nulls first) = 1
)
select
    id,
    method,
    channel,
    shipping_fee,
    row_seq,
    amount_level,
    case when method rlike '^(card|bank_transfer)$' then 'PRIORITY' else 'OTHER_ZONE' end as method_zone
from deduped;
