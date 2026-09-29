select city, 'supplier' source from suppliers where country not in ('DE', 'SG') union all select city, 'store' source from stores where sqft = 9795 order by source;
