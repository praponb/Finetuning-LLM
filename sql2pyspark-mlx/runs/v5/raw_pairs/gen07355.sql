select *, round(sqft / sum(sqft) over (partition by region), 4) sqft_share, ntile(10) over (order by sqft desc, id asc) sqft_bucket from stores
