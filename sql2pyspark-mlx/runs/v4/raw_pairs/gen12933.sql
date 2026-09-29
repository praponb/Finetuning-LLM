select country, 'supplier' as source
from suppliers
union all
select country, 'session' as source
from sessions
