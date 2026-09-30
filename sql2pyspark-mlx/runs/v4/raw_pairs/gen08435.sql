select location, 'department' as source
from departments
union
select city, 'supplier' as source
from suppliers;
