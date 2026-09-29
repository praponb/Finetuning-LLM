SELECT *, COALESCE(region, 'N/A') AS region_filled, ROUND(sqft, 1) AS sqft_rounded FROM stores;
