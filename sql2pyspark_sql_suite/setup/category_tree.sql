-- @id: setup:category_tree
CREATE OR REPLACE TEMP VIEW category_tree AS
SELECT * FROM VALUES
  (1,   CAST(NULL AS INT), 'All Products'),
  (10,  1,  'Consumer Tech'), (11, 10, 'Electronics'), (111, 11, 'audio'), (112, 11, 'camera'), (113, 11, 'accessory'),
  (20,  1,  'Lifestyle'),
  (21,  20, 'Fashion'),       (211, 21, 'tops'),       (212, 21, 'footwear'),
  (22,  20, 'Home'),          (221, 22, 'kitchen'),    (222, 22, 'appliance'),
  (23,  20, 'Beauty'),        (231, 23, 'skincare'),   (232, 23, 'suncare'),
  (30,  1,  'Media'),         (31,  30, 'Books'),      (311, 31, 'tech'),     (312, 31, 'fiction'),
  (40,  1,  'Clearance'),
  (99,  98, 'Legacy Imports')
AS category_tree(node_id, parent_id, node_name);
