select sqft, store_name, cast(sqft as double) as sqft_num
from stores
where not (sqft between 7132 and 12280 and region = 'East')
