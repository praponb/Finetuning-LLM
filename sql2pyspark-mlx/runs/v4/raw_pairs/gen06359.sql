SELECT id, country, segment, QUARTER(signup_date) signup_date_quarter, LOWER(segment) segment_lower FROM customers WHERE segment IS NOT NULL ORDER BY segment_lower, country DESC
