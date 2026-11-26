-- @id: setup:web_events
CREATE OR REPLACE TEMP VIEW web_events AS
SELECT * FROM VALUES
  (1,  1, TIMESTAMP'2024-01-03 09:50:00', 'page_view',   '{"page":"/home","referrer":"google","device":{"os":"iOS","type":"mobile"}}'),
  (2,  1, TIMESTAMP'2024-01-03 09:55:00', 'search',      '{"page":"/search","query":"earbuds","results":24,"device":{"os":"iOS","type":"mobile"}}'),
  (3,  1, TIMESTAMP'2024-01-03 10:02:00', 'product_view','{"page":"/p/101","product_id":101,"device":{"os":"iOS","type":"mobile"}}'),
  (4,  1, TIMESTAMP'2024-01-03 10:10:00', 'add_to_cart', '{"page":"/p/101","product_id":101,"qty":1,"device":{"os":"iOS","type":"mobile"}}'),
  (5,  1, TIMESTAMP'2024-01-03 10:14:30', 'checkout',    '{"page":"/checkout","cart_value":7130,"device":{"os":"iOS","type":"mobile"}}'),
  (6,  1, TIMESTAMP'2024-01-03 15:00:00', 'page_view',   '{"page":"/home","referrer":"direct","device":{"os":"macOS","type":"desktop"}}'),
  (7,  2, TIMESTAMP'2024-02-15 07:40:00', 'page_view',   '{"page":"/home","referrer":"facebook","device":{"os":"Android","type":"mobile"}}'),
  (8,  2, TIMESTAMP'2024-02-15 07:58:00', 'product_view','{"page":"/p/202","product_id":202,"device":{"os":"Android","type":"mobile"}}'),
  (9,  2, TIMESTAMP'2024-02-15 08:01:00', 'add_to_cart', '{"page":"/p/202","product_id":202,"qty":1,"device":{"os":"Android","type":"mobile"}}'),
  (10, 2, TIMESTAMP'2024-02-15 08:04:00', 'checkout',    '{"page":"/checkout","cart_value":3100,"device":{"os":"Android","type":"mobile"}}'),
  (11, 3, TIMESTAMP'2024-03-01 11:20:00', 'search',      '{"page":"/search","query":"action camera 4k","results":7,"device":{"os":"Windows","type":"desktop"}}'),
  (12, 3, TIMESTAMP'2024-03-01 11:25:00', 'product_view','{"page":"/p/102","product_id":102,"device":{"os":"Windows","type":"desktop"}}'),
  (13, 3, TIMESTAMP'2024-03-01 11:40:00', 'add_to_cart', '{"page":"/p/102","product_id":102,"qty":1,"device":{"os":"Windows","type":"desktop"}}'),
  (14, 3, TIMESTAMP'2024-03-01 11:59:00', 'checkout',    '{"page":"/checkout","cart_value":627.5,"device":{"os":"Windows","type":"desktop"}}'),
  (15, 4, TIMESTAMP'2024-04-01 22:00:00', 'page_view',   '{"page":"/home","referrer":"tiktok","device":{"os":"Android","type":"mobile"}}'),
  (16, 4, TIMESTAMP'2024-04-02 09:10:00', 'product_view','{"page":"/p/301","product_id":301,"device":{"os":"Android","type":"mobile"}}'),
  (17, 4, TIMESTAMP'2024-04-02 09:20:00', 'add_to_cart', '{"page":"/p/301","product_id":301,"qty":2,"device":{"os":"Android","type":"mobile"}}'),
  (18, 8, TIMESTAMP'2024-06-12 15:30:00', 'search',      '{"page":"/search","query":"tai nghe","results":0,"device":{"os":"Android","type":"mobile"}}'),
  (19, 8, TIMESTAMP'2024-06-12 15:45:00', 'product_view','{"page":"/p/101","product_id":101,"device":{"os":"Android","type":"mobile"}}'),
  (20, 8, TIMESTAMP'2024-06-12 15:58:00', 'checkout',    '{"page":"/checkout","cart_value":3200000,"device":{"os":"Android","type":"mobile"}}'),
  (21, NULL, TIMESTAMP'2024-06-13 01:00:00', 'page_view', '{"page":"/home","referrer":"bot","device":{"os":"Linux","type":"crawler"}}'),
  (22, 9, TIMESTAMP'2024-06-20 21:00:00', 'page_view',   'not-valid-json'),
  (23, 9, TIMESTAMP'2024-06-20 21:10:00', 'product_view','{"page":"/p/201","product_id":201,"device":{"os":"iOS","type":"tablet"}}'),
  (24, 9, TIMESTAMP'2024-06-20 21:28:00', 'checkout',    '{"page":"/checkout","cart_value":535,"device":{"os":"iOS","type":"tablet"}}')
AS web_events(event_id, customer_id, event_ts, event_type, payload);
