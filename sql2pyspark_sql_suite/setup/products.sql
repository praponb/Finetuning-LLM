-- @id: setup:products
CREATE OR REPLACE TEMP VIEW products AS
SELECT * FROM VALUES
  (101, 'SKU-EL-0001', 'Wireless Earbuds Pro',      'electronics', 'audio',      'Sonix',   CAST(129.90 AS DECIMAL(10,2)), '{"color":"black","weight_g":55,"tags":["bluetooth","anc"],"warranty":{"months":12}}'),
  (102, 'SKU-EL-0002', '4K Action Camera',          'electronics', 'camera',     'Vista',   CAST(349.00 AS DECIMAL(10,2)), '{"color":"grey","weight_g":120,"tags":["4k","waterproof"],"warranty":{"months":24}}'),
  (103, 'SKU-EL-0003', 'USB-C Charger 65W',         'electronics', 'accessory',  'Voltix',  CAST(39.50  AS DECIMAL(10,2)), '{"color":"white","weight_g":110,"tags":["gan"],"warranty":{"months":6}}'),
  (201, 'SKU-FA-0001', 'Linen Shirt',               'fashion',     'tops',       'Kora',    CAST(45.00  AS DECIMAL(10,2)), '{"color":"beige","sizes":["S","M","L"],"tags":["summer"]}'),
  (202, 'SKU-FA-0002', 'Running Shoes',             'fashion',     'footwear',   'Strida',  CAST(89.99  AS DECIMAL(10,2)), '{"color":"blue","sizes":["40","41","42","43"],"tags":["sport","mesh"]}'),
  (301, 'SKU-HO-0001', 'Ceramic Pour-Over Set',     'home',        'kitchen',    'Mori',    CAST(32.00  AS DECIMAL(10,2)), '{"color":"white","tags":["coffee"]}'),
  (302, 'SKU-HO-0002', 'Air Purifier Mini',         'home',        'appliance',  'Breeze',  CAST(159.00 AS DECIMAL(10,2)), '{"color":"white","weight_g":2100,"tags":["hepa"],"warranty":{"months":24}}'),
  (401, 'SKU-BE-0001', 'Vitamin C Serum',           'beauty',      'skincare',   'Glowa',   CAST(24.90  AS DECIMAL(10,2)), '{"volume_ml":30,"tags":["vegan"]}'),
  (402, 'SKU-BE-0002', 'Mineral Sunscreen SPF50',   'beauty',      'suncare',    'Glowa',   CAST(19.90  AS DECIMAL(10,2)), '{"volume_ml":50,"tags":["reef-safe","vegan"]}'),
  (501, 'SKU-BO-0001', 'Data Engineering Handbook', 'books',       'tech',       'Readly',  CAST(55.00  AS DECIMAL(10,2)), '{"pages":640,"tags":["paperback"]}'),
  (999, 'SKU-XX-9999', 'Discontinued Widget',       'misc',        NULL,         NULL,      CAST(1.00   AS DECIMAL(10,2)), NULL)
AS products(product_id, sku, product_name, category, sub_category, brand, list_price, attributes_json);
