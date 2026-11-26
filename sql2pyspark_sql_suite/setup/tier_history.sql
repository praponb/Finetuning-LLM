-- @id: setup:tier_history
CREATE OR REPLACE TEMP VIEW tier_history AS
SELECT * FROM VALUES
  (1, 'BRONZE',   DATE'2022-01-15', 'signup'), (1, 'SILVER',   DATE'2023-03-01', 'auto'),  (1, 'GOLD', DATE'2024-02-01', 'auto'),
  (2, 'BRONZE',   DATE'2022-03-02', 'signup'), (2, 'SILVER',   DATE'2024-01-01', 'auto'),
  (3, 'GOLD',     DATE'2021-11-20', 'signup'), (3, 'PLATINUM', DATE'2023-06-01', 'auto'),  (3, 'PLATINUM', DATE'2024-03-01', 'manual'),
  (4, 'BRONZE',   DATE'2023-06-10', 'signup'),
  (5, CAST(NULL AS STRING), DATE'2023-07-01', 'signup'), (5, CAST(NULL AS STRING), DATE'2023-12-01', 'auto'), (5, 'BRONZE', DATE'2024-05-01', 'manual'),
  (6, 'BRONZE',   DATE'2024-02-29', 'signup'), (6, 'SILVER',   DATE'2024-06-01', 'auto'),
  (7, 'SILVER',   DATE'2022-08-08', 'signup'), (7, 'GOLD',     DATE'2024-01-01', 'auto'),  (7, 'SILVER', DATE'2024-05-15', 'auto'), (7, 'GOLD', DATE'2024-06-15', 'manual'),
  (8, 'BRONZE',   DATE'2024-05-05', 'signup'),
  (9, 'PLATINUM', DATE'2021-01-01', 'signup')
AS tier_history(customer_id, tier, effective_from, change_source);

-- Category hierarchy (depth 0..3). Leaves match products.sub_category within the
-- parent matching products.category. Node 99 is an orphan (parent 98 doesn't exist);
