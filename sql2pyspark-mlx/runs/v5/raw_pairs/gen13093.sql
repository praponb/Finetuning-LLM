select date_trunc('MONTH', paid_at) as paid_month, count(*) as row_count, concat_ws(', ', sort_array(collect_set(status))) as status_values
from payments
where amount between 580 and 1470
group by date_trunc('MONTH', paid_at)
having row_count > 1
order by status_values desc
