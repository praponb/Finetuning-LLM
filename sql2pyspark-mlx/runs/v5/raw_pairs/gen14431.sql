select channel
from promotions
except distinct
select channel
from promotions
where starts_on <= '2024-08-01';
