select case when amount >= 1350 then 'high' when amount >= 580 then 'mid' else 'low' end amount_bucket, method, count(case when status = 'FAILED' then 1 end) failed_count
from payments
where status in ('SUCCESS', 'REFUNDED', 'FAILED')
group by 1, 2
having count(case when status = 'FAILED' then 1 end) >= 5
order by amount_bucket desc, failed_count desc
