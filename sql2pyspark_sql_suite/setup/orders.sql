-- @id: setup:orders
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1001, 1, TIMESTAMP'2024-01-03 10:15:00', 'DELIVERED',  'web',    'THB', named_struct('street','12 Sukhumvit 21','city','Bangkok','zip','10110')),
  (1002, 1, TIMESTAMP'2024-02-14 20:41:00', 'DELIVERED',  'app',    'THB', named_struct('street','12 Sukhumvit 21','city','Bangkok','zip','10110')),
  (1003, 2, TIMESTAMP'2024-02-15 08:05:00', 'CANCELLED',  'web',    'THB', named_struct('street','5 Nimman Rd','city','Chiang Mai','zip','50200')),
  (1004, 3, TIMESTAMP'2024-03-01 12:00:00', 'DELIVERED',  'app',    'SGD', named_struct('street','1 Orchard Rd','city','Singapore','zip','238801')),
  (1005, 3, TIMESTAMP'2024-03-20 23:59:59', 'RETURNED',   'app',    'SGD', named_struct('street','1 Orchard Rd','city','Singapore','zip','238801')),
  (1006, 4, TIMESTAMP'2024-04-02 09:30:00', 'DELIVERED',  'market', 'IDR', named_struct('street','Jl. Sudirman 8','city','Jakarta','zip','10220')),
  (1007, 5, TIMESTAMP'2024-04-18 14:22:00', 'SHIPPED',    'web',    'IDR', NULL),
  (1008, 6, TIMESTAMP'2024-05-05 19:00:00', 'DELIVERED',  'app',    'THB', named_struct('street','88 Silom','city','Bangkok','zip','10500')),
  (1009, 7, TIMESTAMP'2024-05-30 07:45:00', 'DELIVERED',  'web',    'PHP', named_struct('street','23 Ayala Ave','city','Makati','zip','1226')),
  (1010, 1, TIMESTAMP'2024-06-11 11:11:00', 'DELIVERED',  'web',    'THB', named_struct('street','12 Sukhumvit 21','city','Bangkok','zip','10110')),
  (1011, 8, TIMESTAMP'2024-06-12 16:00:00', 'PENDING',    'app',    'VND', named_struct('street','15 Hang Bai','city','Hanoi','zip','100000')),
  (1012, 9, TIMESTAMP'2024-06-20 21:30:00', 'DELIVERED',  'market', 'MYR', named_struct('street','3 Jalan Ampang','city','Kuala Lumpur','zip','50450')),
  (1013, 9, TIMESTAMP'2024-07-01 10:00:00', 'DELIVERED',  'app',    'MYR', named_struct('street','3 Jalan Ampang','city','Kuala Lumpur','zip','50450')),
  (1014, 3, TIMESTAMP'2024-07-04 13:13:13', 'DELIVERED',  'web',    'SGD', named_struct('street','1 Orchard Rd','city','Singapore','zip','238801')),
  (1015, 6, TIMESTAMP'2024-07-15 18:20:00', 'SHIPPED',    'app',    'THB', named_struct('street','88 Silom','city','Bangkok','zip','10500')),
  (1016, 2, TIMESTAMP'2024-07-20 09:00:00', 'DELIVERED',  'web',    'THB', named_struct('street','5 Nimman Rd','city','Chiang Mai','zip','50200'))
AS orders(order_id, customer_id, order_ts, status, channel, currency, ship_to);
