with latest_match as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.payment_amount,
        top1.method
    from orders s
    left join lateral (
            select c.amount as payment_amount, c.method
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from latest_match e
    left join promotions pr
        on coalesce(e.channel, 'web') = pr.channel
       and e.order_date >= pr.starts_on
       and e.order_date <= date_add(pr.ends_on, 7)
),
enriched as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by customer_id order by order_date, id, promo_id) as row_seq,
        sum(discount) over w_running as cumulative_discount,
        dense_rank() over (partition by status order by shipping_fee nulls last) as shipping_fee_dense_rank
    from matched
    window w_running as (partition by status, channel order by discount, id, promo_id rows between unbounded preceding and current row)
)
-- Aggregate with conditional metrics
select
    store_id,
    count(case when promo_code = 'BUNDLE' then 1 end) as bundle_count,
    count(*) as row_count
from enriched
group by store_id
having count(*) < 1
order by row_count nulls first
