select round(stddev(quantity), 2) as stddev_quantity, count(*) as row_count
from order_items
order by row_count, stddev_quantity desc;
