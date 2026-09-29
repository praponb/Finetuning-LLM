select status, case when method in ('paypal', 'card') then 'Group A' else 'Other' end method_group
from payments
where status = 'FAILED' and paid_at >= '2025-09-12'
order by method_group desc
