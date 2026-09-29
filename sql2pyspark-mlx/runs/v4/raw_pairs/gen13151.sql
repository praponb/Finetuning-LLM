select opened_date, store_name
from stores
where store_name is not null
except
select opened_date, store_name
from stores
where opened_date >= '2025-02-11';
