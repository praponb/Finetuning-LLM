select region, max(sqft) - min(sqft) sqft_range
from stores
group by 1
order by region;
