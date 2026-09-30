select
    case when discount >= 0.39 then 'high' when discount >= 0.16 then 'mid' else 'low' end as discount_bucket,
    status,
    channel,
    round((100 * sum(case when channel = 'partner' then 1 else 0 end)) / count(*), 1) as pct_partner,
    sum(case when status = 'COMPLETED' then discount else 0 end) as completed_discount
from orders
where shipping_fee > 27 and (channel != 'partner' or amount <= 1260)
group by case when discount >= 0.39 then 'high' when discount >= 0.16 then 'mid' else 'low' end, status, channel
having round((100 * sum(case when channel = 'partner' then 1 else 0 end)) / count(*), 1) >= 47;
