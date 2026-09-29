select to_date(paid_at) as paid_day, sum(case when method = 'promptpay' then amount else 0 end) as promptpay_amount, count(case when status = 'REFUNDED' then 1 end) as refunded_count
from payments
where amount >= 530
group by to_date(paid_at);
