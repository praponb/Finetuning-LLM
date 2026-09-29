select o.channel, o.amount, s.region, s.city, s.store_name, coalesce(s.sqft, 0) as sqft_filled from orders o left join stores s on o.store_id = s.id where s.opened_date <= '2025-08-23';
