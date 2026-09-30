select * from (select is_flagged, merchant, max(amount) - min(amount) amount_range from transactions group by is_flagged, merchant) t where amount_range > 49 order by amount_range desc;
