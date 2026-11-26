-- Q18c: Ticket SLA in Bangkok local business hours (Mon-Fri 09:00-18:00)
-- needs setup views: support_tickets
-- @id: Q18c
WITH local AS (
  SELECT
    ticket_id, priority, channel,
    from_utc_timestamp(opened_ts, 'Asia/Bangkok')                                  AS opened_bkk,
    from_utc_timestamp(coalesce(first_response_ts, resolved_ts, TIMESTAMP'2024-08-01 00:00:00'), 'Asia/Bangkok') AS responded_bkk,
    first_response_ts IS NULL                                                       AS no_response
  FROM support_tickets
),
hours AS (
  SELECT
    l.*,
    sequence(date_trunc('HOUR', opened_bkk), date_trunc('HOUR', responded_bkk), INTERVAL 1 HOUR) AS hour_starts
  FROM local l
),
biz AS (
  SELECT
    h.*,
    aggregate(
      filter(hour_starts, hs -> dayofweek(hs) BETWEEN 2 AND 6 AND hour(hs) BETWEEN 9 AND 17),
      0L,
      (acc, hs) -> acc + greatest(0L,
                     timestampdiff(MINUTE,
                                   greatest(hs, opened_bkk),
                                   least(hs + INTERVAL 1 HOUR, responded_bkk)))
    )                                                                               AS business_minutes
  FROM hours h
)
SELECT
  ticket_id,
  priority,
  date_format(opened_bkk, 'EEE yyyy-MM-dd HH:mm')                                  AS opened_local,
  dayofweek(opened_bkk) IN (1, 7)                                                  AS opened_on_weekend,
  hour(opened_bkk) NOT BETWEEN 9 AND 17                                            AS opened_after_hours,
  timestampdiff(MINUTE, opened_bkk, responded_bkk)                                 AS wall_clock_minutes,
  business_minutes,
  no_response,
  CASE priority WHEN 'P1' THEN 60 WHEN 'P2' THEN 240 WHEN 'P3' THEN 540 ELSE 1080 END AS sla_business_minutes,
  business_minutes > CASE priority WHEN 'P1' THEN 60 WHEN 'P2' THEN 240 WHEN 'P3' THEN 540 ELSE 1080 END AS sla_breached,
  CASE WHEN dayofweek(opened_bkk) IN (1, 7) THEN next_day(to_date(opened_bkk), 'MON')
       ELSE to_date(opened_bkk) END                                                AS first_business_day,
  last_day(opened_bkk)                                                             AS month_end,
  add_months(trunc(opened_bkk, 'QUARTER'), 3)                                      AS next_quarter_start,
  weekofyear(opened_bkk)                                                           AS iso_week,
  extract(DOY FROM opened_bkk)                                                     AS day_of_year,
  make_timestamp(year(opened_bkk), month(opened_bkk), day(opened_bkk), 18, 0, 0)  AS same_day_cutoff,
  size(hour_starts)                                                                AS hours_spanned
FROM biz
ORDER BY ticket_id;
