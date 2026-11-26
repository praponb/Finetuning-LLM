-- @id: setup:customers
CREATE OR REPLACE TEMP VIEW customers AS
SELECT * FROM VALUES
  (1,  'Alice Tan',       'alice.tan@example.com',   'TH', 'Bangkok',     DATE'2022-01-15', 'GOLD',     map('newsletter','Y','sms','N'),  array('electronics','books')),
  (2,  'Bob Srisai',      'bob.s@example.co.th',     'TH', 'Chiang Mai',  DATE'2022-03-02', 'SILVER',   map('newsletter','N','sms','Y'),  array('fashion')),
  (3,  'Chen Wei',        'chen.wei@example.sg',     'SG', 'Singapore',   DATE'2021-11-20', 'PLATINUM', map('newsletter','Y','sms','Y'),  array('electronics','home','beauty')),
  (4,  'Dewi Lestari',    'dewi@example.id',         'ID', 'Jakarta',     DATE'2023-06-10', 'BRONZE',   map('newsletter','Y'),            array()),
  (5,  'Eko Prasetyo',    NULL,                      'ID', 'Surabaya',    DATE'2023-07-01', NULL,       map(),                            array('home')),
  (6,  'Fah Wongsa',      'fah.w@example.com',       'TH', 'Bangkok',     DATE'2024-02-29', 'SILVER',   map('sms','Y'),                   array('beauty','fashion')),
  (7,  'Gina Reyes',      'GINA.REYES@EXAMPLE.PH',   'PH', 'Manila',      DATE'2022-08-08', 'GOLD',     map('newsletter','N'),            array('books','home')),
  (8,  'Hoang Nguyen',    'hoang.n@example.vn',      'VN', 'Hanoi',       DATE'2024-05-05', 'BRONZE',   map('newsletter','Y','sms','N'),  array('electronics')),
  (9,  'Ivy Lim',         'ivy.lim@example.my',      'MY', 'Kuala Lumpur',DATE'2021-01-01', 'PLATINUM', map('newsletter','Y','sms','Y'),  array('fashion','beauty','home')),
  (10, 'Jon Inactive',    'jon@example.com',         'TH', 'Phuket',      DATE'2020-12-12', 'BRONZE',   map(),                            array())
AS customers(customer_id, full_name, email, country_code, city, signup_date, tier, preferences, interests);
