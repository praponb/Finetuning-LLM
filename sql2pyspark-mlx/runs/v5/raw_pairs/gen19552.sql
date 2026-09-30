with filtered_payments as (
    select *
    from payments
    where amount < 780
),
order_id_summary as (
    select order_id, round((100 * sum(case when status = 'SUCCESS' then 1 else 0 end)) / count(*), 1) as pct_success, max(amount) as max_amount
    from filtered_payments
    group by order_id
)
select *
from order_id_summary;
