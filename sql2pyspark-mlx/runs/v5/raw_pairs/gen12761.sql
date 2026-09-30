select
    store_id,
    bit_and(units) as units_bit_and,
    bit_or(units) as units_bit_or,
    count(*) as num_rows
from store_sales
group by store_id
having units_bit_and > 1
order by units_bit_and desc nulls last
