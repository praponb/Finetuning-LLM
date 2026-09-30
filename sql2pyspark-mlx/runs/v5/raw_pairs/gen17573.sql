select sum(amount) as total_amount, count(case when status = 'REFUNDED' then 1 end) as refunded_count
from payments
where method in ('cash', 'bank_transfer') and method = 'card';
