with src as (
    select *, case when method in ('bank_transfer', 'cash') then 'Tier 1' else 'Other' end as method_group
    from payments
    where method in ('card', 'paypal') and amount > 1120 and order_id is not null
),
summary as (
    select quarter(paid_at) as paid_quarter, count(case when status = 'FAILED' then 1 end) as failed_count
    from src
    group by quarter(paid_at)
)
select paid_quarter, failed_count
from summary
