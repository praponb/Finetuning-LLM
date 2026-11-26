-- @id: setup:promotions
CREATE OR REPLACE TEMP VIEW promotions AS
SELECT * FROM VALUES
  ('NEWYEAR24', DATE'2024-01-01', DATE'2024-01-07', array('electronics','books'), 0.10),
  ('VALENTINE', DATE'2024-02-10', DATE'2024-02-15', array('beauty','fashion'),    0.15),
  ('SONGKRAN',  DATE'2024-04-10', DATE'2024-04-16', array('home','fashion'),      0.12),
  ('MIDYEAR',   DATE'2024-06-01', DATE'2024-07-15', array(),                      0.08)
AS promotions(promo_code, start_date, end_date, eligible_categories, promo_pct);
