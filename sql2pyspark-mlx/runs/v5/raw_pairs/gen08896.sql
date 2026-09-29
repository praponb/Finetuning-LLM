SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY delivered_date DESC NULLS LAST, id ASC) AS rank_in_group FROM shipments QUALIFY rank_in_group = 1
