select 'employee' as record_type, hire_date
from employees
union
select 'shipment' as record_type, shipped_date
from shipments
where status not in ('IN_TRANSIT', 'LOST')
order by hire_date
