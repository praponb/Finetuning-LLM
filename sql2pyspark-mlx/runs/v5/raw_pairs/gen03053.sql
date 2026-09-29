select sale_date
from store_sales
except
select opened_date
from accounts;
