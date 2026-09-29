SELECT YEAR(shipped_date) shipped_year, QUARTER(delivered_date) delivered_quarter, MAX(cost) - MIN(cost) cost_range FROM shipments GROUP BY 1, 2 ORDER BY delivered_quarter;
