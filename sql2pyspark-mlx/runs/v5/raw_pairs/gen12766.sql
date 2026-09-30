select city as label
from customers
where city not in ('Chiang Mai', 'London', 'Tokyo')
except
select location as label
from departments
where location in ('Berlin', 'Tokyo', 'London')
