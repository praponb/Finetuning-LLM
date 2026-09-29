select s.cost, s.carrier, o.amount, st.city, cast(st.sqft as string) as sqft_str from shipments s left outer join orders o on o.id = s.order_id inner join stores st on o.store_id = st.id;
