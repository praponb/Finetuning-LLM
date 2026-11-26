-- Q17b: Tag analytics: explode, positional tags, per-tag SLA and satisfaction
-- needs setup views: support_tickets
-- @id: Q17b
WITH sla AS (
  SELECT
    t.*,
    map('P1', 60, 'P2', 240, 'P3', 1440, 'P4', 2880)[priority]               AS sla_minutes,
    timestampdiff(MINUTE, opened_ts, first_response_ts)                       AS response_minutes
  FROM support_tickets t
)
SELECT
  tg.tag,
  COUNT(*)                                                                    AS tickets,
  count_if(tg.pos = 0)                                                        AS times_primary_tag,
  array_sort(collect_set(nullif(lower(trim(s.agent_email)), '')))             AS agents,
  round(avg(s.csat), 2)                                                       AS avg_csat,
  COUNT(s.csat)                                                               AS csat_responses,
  count_if(s.response_minutes IS NULL OR s.response_minutes > s.sla_minutes)  AS sla_breaches,
  percentile(s.response_minutes, 0.5)                                         AS median_response_min,
  MAX(s.response_minutes)                                                     AS worst_response_min,
  concat_ws(',', array_sort(collect_list(s.ticket_id)))                       AS ticket_ids,
  min(s.priority)                                                             AS top_priority
FROM sla s
LATERAL VIEW posexplode(split(s.tags, ';')) tg AS pos, tag
GROUP BY tg.tag
HAVING COUNT(*) >= 1
ORDER BY tickets DESC, tg.tag;
