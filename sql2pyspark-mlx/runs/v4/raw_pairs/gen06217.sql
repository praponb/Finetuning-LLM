SELECT id, city, sqft, CAST(sqft AS STRING) AS sqft_str FROM stores WHERE opened_date >= '2023-10-11' AND sqft >= 12668 ORDER BY sqft_str, sqft DESC
