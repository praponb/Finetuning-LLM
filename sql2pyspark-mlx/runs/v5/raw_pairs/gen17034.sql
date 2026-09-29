select id, tags, size(tags) as tags_count
from products
where price < 420 and cost >= 470
