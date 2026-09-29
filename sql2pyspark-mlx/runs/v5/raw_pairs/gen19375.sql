with matched as (
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from orders src
    left join promotions pr
        on src.channel <=> pr.channel
       and src.order_date >= pr.starts_on
       and src.order_date < pr.ends_on + interval '14' day
)
select
    date_trunc('year', order_date) as year_start,
    store_id,
    round(sum(discount_pct) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as discount_pct_per_partner,
    count(case when channel = 'mobile' then 1 end) as mobile_count,
    min(discount) as min_discount,
    count(*) filter (where status = 'CANCELLED') as cancelled_rows,
    sum(case when status = 'SHIPPED' then shipping_fee else 0 end) as shipped_shipping_fee,
    stddev_samp(amount) as amount_stddev
from matched
group by cube (date_trunc('year', order_date), store_id)
having count(*) filter (where status = 'CANCELLED') > 1
