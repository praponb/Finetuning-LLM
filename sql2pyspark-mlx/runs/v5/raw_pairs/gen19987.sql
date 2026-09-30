select id
from stores
intersect
select id
from stores
where region != 'East';
