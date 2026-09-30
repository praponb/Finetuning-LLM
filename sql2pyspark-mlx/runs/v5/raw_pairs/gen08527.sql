SELECT id, region, store_name, CEIL(sqft) AS sqft_whole, CEIL(sqft) AS sqft_whole_2 FROM stores WHERE sqft > 10551
