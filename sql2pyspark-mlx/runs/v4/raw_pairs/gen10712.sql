SELECT sqft, region, UPPER(region) AS region_upper FROM stores WHERE sqft > 10540 ORDER BY region, region_upper DESC
