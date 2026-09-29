select count(distinct price) as unique_categorys, count(case when category = 'Sports' then 1 end) as sports_count
from products
order by unique_categorys asc, 2 desc
