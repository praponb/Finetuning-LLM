select id, opened_date, sqft, region from stores where sqft != 10816 and (sqft <= 15646 or store_name is not null)
