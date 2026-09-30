select quarter(end_date) end_quarter, concat_ws(', ', sort_array(collect_set(status))) status_values
from subscriptions
group by quarter(end_date)
order by end_quarter desc;
