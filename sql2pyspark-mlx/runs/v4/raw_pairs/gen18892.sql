SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE cost > 32)
PIVOT (MAX(cost) FOR carrier IN ('DHL', 'Kerry'));
