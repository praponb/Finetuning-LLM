-- Q17a: One row per ticket, every field cleaned / extracted
-- needs setup views: payments, support_tickets
-- @id: Q17a
WITH t AS (
  SELECT
    *,
    nullif(lower(trim(agent_email)), '')                                       AS agent_email_clean,
    str_to_map(meta, ';', '=')                                                 AS meta_map
  FROM support_tickets
),
parsed AS (
  SELECT
    t.*,
    initcap(translate(substring_index(agent_email_clean, '@', 1), '._', '  ')) AS agent_name,
    regexp_replace(subject, '(?i)^((re|fw|fwd):\\s*)+', '')                    AS subject_core,
    length(subject) - length(replace(subject, '!', ''))                       AS exclamations,
    array_distinct(transform(
      regexp_extract_all(body, '(?i)(?:ord-|order\\s*#?\\s*)(\\d{4})', 1),
      x -> CAST(x AS INT)))                                                    AS order_refs,
    regexp_extract(body, '(\\+\\d{2}-\\d{2}-\\d{3}-\\d{4})', 1)                 AS phone_raw,
    regexp_extract(body, '([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,})', 1) AS email_in_body,
    regexp_extract(body, '\\*{4}(\\d{4})', 1)                                   AS card_last4,
    split(tags, ';')                                                           AS tag_arr,
    element_at(meta_map, 'lang')                                               AS lang,
    coalesce(CAST(element_at(meta_map, 'vip') AS INT), 0) = 1                  AS is_vip,
    parse_url(element_at(meta_map, 'utm'), 'QUERY', 'utm_source')             AS utm_source
  FROM t
)
SELECT
  ticket_id,
  customer_id,
  left(sha2(concat_ws('|', ticket_id, coalesce(CAST(customer_id AS STRING), 'anon')), 256), 16) AS anon_key,
  agent_email_clean,
  agent_name,
  subject_core,
  upper(subject_core) = subject_core OR subject RLIKE '\\b[A-Z]{4,}\\b'       AS is_shouting,
  exclamations,
  order_refs,
  size(order_refs)                                                           AS n_order_refs,
  -- look-ahead: mask every digit that still has 4+ digits after it
  -- regexp_extract returns '' (not NULL) when nothing matches
  regexp_replace(nullif(phone_raw, ''), '\\d(?=.*\\d{4}$)', '#')            AS phone_masked,
  nullif(email_in_body, '')                                                  AS email_in_body,
  regexp_replace(body, '\\*{4}(\\d{4})', 'card-x$1')                         AS body_redacted,
  nullif(card_last4, '')                                                     AS card_last4,
  tag_arr,
  array_contains(tag_arr, 'defect')                                          AS is_defect,
  lang,
  is_vip,
  utm_source,
  length(body)                                                               AS body_chars,
  octet_length(body)                                                         AS body_bytes,
  body RLIKE '[\\x{0E00}-\\x{0E7F}]'                                          AS has_thai,
  size(filter(split(lower(body), '[^a-z]+'), w -> w IN ('please', 'thanks', 'thank'))) AS politeness_words,
  levenshtein(lower(subject_core), 'where is my order?') <= 3                 AS is_wismo,
  CASE
    WHEN lower(body) LIKE ANY ('%refund%', '%return%')         THEN 'returns'
    WHEN body ILIKE '%cancel%'                                 THEN 'cancellation'
    WHEN lower(body) RLIKE 'payment|card|wallet'               THEN 'payments'
    WHEN tags LIKE '%presales%'                                THEN 'presales'
    WHEN subject LIKE 'RE!_%' ESCAPE '!' OR body = 'unsubscribe' THEN 'noise'
    ELSE 'product'
  END                                                                        AS topic,
  format_string('%s|%-5s|%3d chars', priority, channel, length(body))         AS summary_line,
  lpad(CAST(coalesce(csat, 0) AS STRING), 3, '0')                           AS csat_padded,
  overlay(ticket_id PLACING 'TKT' FROM 1 FOR 1)                               AS ticket_alt_id
FROM parsed
ORDER BY ticket_id;
