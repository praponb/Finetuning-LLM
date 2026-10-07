"""A synthetic table catalog for v15: ~200 tables in 36 domains with names the model has never seen.

Why: every dataset up to v14 used the same 42 tables (17 classic + 5 nested + 20 complex_v8, 191 column names). v13 and
v14 then copied training names into unseen hold-out schemas (`df_patents`, `apppt_date`, `num_stores` for `num_stops`,
`ss.restock` for `ss.revenue`). With these tables the query forms stay the same but the names change, so copying them
from the SQL is the only strategy that works.

The tables use the classic `schemas.Table` / `Column` types, so `schemas.make_rows` fills them (40 seeded rows) and the
existing generators can run on them once `install()` has put them in `schemas` (see patch.py).

* every domain has 4-7 tables; child tables reference a parent with an fk column; each table gets an id, 1-2 categorical
  columns (one non-NULL, usable as a window / group key), 1-3 numbers, a date or timestamp and sometimes a string /
  boolean / string-array column, all drawn (seeded) from the domain's own vocabulary
* name styles vary: plain plurals, `dim_` / `fact_` / `tbl_` prefixes, abbreviations (`qty_shipped`, `amt_due`), long
  names, and near neighbours of training names on purpose (`store_kiosks`, `restock_qty`, `revenue_share`, `stop_seq`)
* nested tables (an ARRAY<STRUCT<...>> column with domain field names + a STRUCT column) for nested_builder.py
* HELDOUT_DOMAINS never reach training: their pairs form the schema-shift eval set
* identifiers of the hold-out tables and the real-world query tables that no training table uses are forbidden
  (checked at import), and no table name repeats a training table name
"""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from schemas import Column, Table

# 36 domains: tables as (name, parent or None), then the domain vocabulary
#   num: name -> (lo, hi, "int" | "double");  cat: name -> values;  dt: date names;  ts: timestamp names
#   txt: name -> values (free-text-like strings);  flag: boolean names;  tags: name -> values (ARRAY<STRING>)
#   nest: (array column, element field spec, struct column, struct field spec) for one nested table
DOMAINS: dict[str, dict] = {
    "energy": dict(
        tables=[("power_plants", None), ("generators", "power_plants"), ("gen_meter_reads", "generators"),
                ("outage_events", "power_plants"), ("dim_grid_zones", None)],
        num={"capacity_mw": (50, 900, "double"), "output_kwh": (0, 5000, "int"), "efficiency_pct": (20, 60, "double"),
             "voltage_kv": (11, 500, "int"), "fuel_cost_usd": (10, 800, "double"), "downtime_min": (0, 600, "int")},
        cat={"fuel_type": ("coal", "gas", "solar", "wind", "hydro"), "zone_code": ("Z1", "Z2", "Z3", "Z4"),
             "unit_state": ("ONLINE", "OFFLINE", "STANDBY"), "outage_cause": ("storm", "fault", "maintenance", "overload")},
        dt=["commissioned_on", "inspected_on"], ts=["reading_at", "outage_started_at"],
        txt={"plant_label": ("North Ridge", "Delta Bay", "Sun Valley", "Iron Creek", "Westgate")},
        flag=["is_renewable", "needs_service"], tags={"cert_codes": ("ISO50001", "LEED", "GREEN", "EU-ETS")},
        nest=("turbines", "turbine_tag:cat:T-A|T-B|T-C|T-D, rpm:int:600:1800, blade_temp:double:20:90",
              "site_geo", "lat_band:cat:N|S, grid_cell:cat:A1|B2|C3")),
    "schools": dict(
        tables=[("campuses", None), ("classrooms", "campuses"), ("pupils", "campuses"), ("exam_scores", "pupils"),
                ("tutors", "campuses"), ("lesson_plans", "tutors")],
        num={"seat_count": (10, 60, "int"), "score_pct": (0, 100, "double"), "grade_level": (1, 12, "int"),
             "tuition_fee": (500, 9000, "double"), "absent_days": (0, 30, "int"), "lesson_minutes": (30, 120, "int")},
        cat={"term_code": ("T1", "T2", "T3"), "subject_area": ("math", "science", "arts", "history", "language"),
             "campus_type": ("public", "private", "charter"), "house_name": ("Red", "Blue", "Green", "Gold")},
        dt=["enrolled_on", "exam_day", "hired_on"], ts=["submitted_at"],
        txt={"tutor_alias": ("Ms. Ploy", "Mr. Tanaka", "Dr. Okafor", "Mrs. Lindqvist", "Mr. Alvarez")},
        flag=["has_scholarship", "is_boarding"], tags={"club_codes": ("chess", "robotics", "choir", "drama", "debate")},
        nest=("assessments", "unit_code:cat:U1|U2|U3|U4, mark:double:0:100, attempt_no:int:1:3",
              "guardian", "relation:cat:mother|father|aunt|grandparent, contact_pref:cat:sms|email|call")),
    "farms": dict(
        tables=[("farm_plots", None), ("crop_cycles", "farm_plots"), ("harvest_logs", "crop_cycles"),
                ("irrigation_runs", "farm_plots"), ("tbl_seed_lots", None)],
        num={"plot_hectares": (0.5, 40, "double"), "yield_kg": (100, 9000, "int"), "water_litres": (500, 20000, "int"),
             "soil_ph": (4.5, 8.5, "double"), "seed_cost": (20, 900, "double"), "germination_pct": (40, 99, "double")},
        cat={"crop_kind": ("rice", "maize", "cassava", "soy", "durian"), "soil_class": ("clay", "loam", "sand", "silt"),
             "season_tag": ("wet", "dry", "cool"), "harvest_grade": ("A", "B", "C")},
        dt=["sown_on", "harvested_on"], ts=["valve_opened_at"],
        txt={"plot_nickname": ("East Paddy", "Hill Row", "Creek Field", "Old Orchard", "Long Strip")},
        flag=["is_organic", "pest_alert"], tags={"input_codes": ("NPK", "urea", "compost", "lime")},
        nest=("sensor_probes", "probe_id:cat:P1|P2|P3, moisture_pct:double:5:60, depth_cm:int:10:80",
              "plot_owner", "owner_type:cat:family|coop|company, region:cat:Isan|North|Central")),
    "gaming": dict(
        tables=[("game_titles", None), ("player_profiles", None), ("match_rounds", "game_titles"),
                ("loot_drops", "match_rounds"), ("guild_rosters", "player_profiles"), ("fact_purchases", "player_profiles")],
        num={"xp_points": (0, 50000, "int"), "kill_count": (0, 40, "int"), "gem_spend": (0, 999, "double"),
             "round_secs": (60, 1800, "int"), "rarity_score": (1, 100, "double"), "ping_ms": (5, 300, "int")},
        cat={"game_mode": ("solo", "duo", "squad", "ranked"), "platform_code": ("PC", "PS", "XB", "MOB"),
             "guild_rank": ("recruit", "member", "officer", "leader"), "drop_tier": ("common", "rare", "epic", "legend")},
        dt=["registered_on", "released_on"], ts=["round_ended_at", "bought_at"],
        txt={"gamer_tag": ("NightOwl", "pixelPanda", "x_Raptor_x", "LunaBee", "ToastyMcToast")},
        flag=["is_banned", "has_battle_pass"], tags={"badge_codes": ("mvp", "first_blood", "clutch", "pacifist")},
        nest=("inventory_slots", "item_code:cat:SWD|BOW|POT|SHD, item_qty:int:1:20, item_value:double:1:500",
              "rig_spec", "gpu_class:cat:low|mid|high, input_kind:cat:pad|kbm|touch")),
    "telecom": dict(
        tables=[("cell_towers", None), ("sim_cards", None), ("call_records", "sim_cards"), ("data_sessions", "sim_cards"),
                ("tower_alarms", "cell_towers"), ("dim_rate_plans", None)],
        num={"call_secs": (1, 3600, "int"), "data_mb": (0, 8000, "double"), "signal_dbm": (-120, -40, "int"),
             "monthly_cap_gb": (1, 200, "int"), "roaming_charge": (0, 90, "double"), "dropped_calls": (0, 9, "int")},
        cat={"network_gen": ("3G", "4G", "5G"), "plan_tier": ("prepaid", "postpaid", "family", "biz"),
             "alarm_level": ("minor", "major", "critical"), "tower_vendor": ("Nokia", "Ericsson", "Huawei", "ZTE")},
        dt=["activated_on", "installed_on"], ts=["call_started_at", "alarm_raised_at"],
        txt={"msisdn_mask": ("66-8x-xxx-1201", "66-9x-xxx-4410", "66-6x-xxx-7782", "66-8x-xxx-0093")},
        flag=["is_roaming", "is_esim"], tags={"addon_codes": ("VOLTE", "HOTSPOT", "INTL", "MUSIC")},
        nest=("hops", "hop_tower:cat:TW1|TW2|TW3|TW4, hop_ms:int:1:120, handover_ok:bool",
              "device_info", "os_family:cat:android|ios|other, band_class:cat:n41|n78|b3")),
    "insurance": dict(
        tables=[("insureds", None), ("cover_contracts", "insureds"), ("loss_notices", "cover_contracts"),
                ("settlement_payouts", "loss_notices"), ("field_assessors", None)],
        num={"premium_amt": (100, 12000, "double"), "claim_amt": (50, 80000, "double"), "deductible": (0, 5000, "int"),
             "risk_score": (1, 100, "int"), "payout_amt": (0, 60000, "double"), "days_to_settle": (1, 180, "int")},
        cat={"cover_type": ("auto", "home", "health", "travel", "life"), "claim_state": ("OPEN", "PAID", "DENIED", "REVIEW"),
             "region_band": ("R1", "R2", "R3"), "adjuster_team": ("alpha", "bravo", "charlie")},
        dt=["policy_start", "loss_date", "paid_on"], ts=["filed_at"],
        txt={"holder_alias": ("Somchai P.", "Ana L.", "Kenji M.", "Fatima Z.", "Olu A.")},
        flag=["is_fraud_flag", "auto_renew"], tags={"rider_codes": ("FLOOD", "THEFT", "DENTAL", "ACCIDENT")},
        nest=("damages", "damage_part:cat:roof|bumper|window|engine, est_cost:double:50:9000, severity:int:1:5",
              "broker", "broker_tier:cat:gold|silver|bronze, channel:cat:agent|online|bank")),
    "realty": dict(
        tables=[("listings", None), ("viewings", "listings"), ("offers", "listings"), ("realtors", None),
                ("rent_ledgers", "listings"), ("fact_valuations", "listings")],
        num={"floor_sqm": (20, 600, "double"), "asking_price": (50000, 2000000, "double"), "bedroom_cnt": (0, 6, "int"),
             "offer_amt": (40000, 2100000, "double"), "monthly_rent": (300, 9000, "double"), "storey_no": (1, 60, "int")},
        cat={"property_kind": ("condo", "house", "townhome", "loft"), "listing_status": ("ACTIVE", "SOLD", "WITHDRAWN"),
             "district_code": ("D01", "D02", "D03", "D04", "D05"), "offer_state": ("pending", "accepted", "rejected")},
        dt=["listed_on", "viewed_on", "valued_on"], ts=["offer_made_at"],
        txt={"street_line": ("12 Soi Ari", "88 Elm St", "4 Rue Clair", "301 Canal Rd", "7 Hillview")},
        flag=["has_parking", "is_furnished"], tags={"amenity_codes": ("pool", "gym", "garden", "lift", "sauna")},
        nest=("rooms", "room_kind:cat:bed|bath|kitchen|living, room_sqm:double:4:60, window_cnt:int:0:4",
              "address_parts", "province:cat:BKK|CNX|PKT, postcode_zone:cat:10|50|83")),
    "sports": dict(
        tables=[("clubs", None), ("athletes", "clubs"), ("fixtures", "clubs"), ("goal_events", "fixtures"),
                ("training_loads", "athletes"), ("dim_venues", None)],
        num={"jersey_no": (1, 99, "int"), "sprint_kmh": (15, 37, "double"), "minutes_played": (0, 120, "int"),
             "attendance": (500, 60000, "int"), "load_au": (100, 1200, "double"), "goal_minute": (1, 95, "int")},
        cat={"position_code": ("GK", "DF", "MF", "FW"), "league_div": ("D1", "D2", "D3"),
             "fixture_result": ("W", "D", "L"), "venue_surface": ("grass", "turf", "hybrid")},
        dt=["signed_on", "fixture_day"], ts=["kickoff_at", "scored_at"],
        txt={"club_motto": ("Never Yield", "Pride of the North", "One Heart", "Rise Again", "Steel and Fire")},
        flag=["is_captain", "is_home"], tags={"injury_codes": ("ACL", "HAM", "ANK", "CON")},
        nest=("splits", "lap_no:int:1:10, split_secs:double:10:80, zone_tag:cat:Z1|Z2|Z3",
              "kit", "kit_color:cat:red|white|black|blue, sponsor_band:cat:A|B|C")),
    "library": dict(
        tables=[("branches", None), ("patrons", "branches"), ("catalog_items", "branches"), ("loans", "patrons"),
                ("holds_queue", "catalog_items"), ("partners", None)],
        num={"page_count": (20, 1200, "int"), "late_fee": (0, 40, "double"), "copies_owned": (1, 12, "int"),
             "loan_days": (1, 60, "int"), "queue_pos": (1, 30, "int"), "donation_amt": (0, 5000, "double")},
        cat={"media_type": ("book", "dvd", "ebook", "audio"), "patron_tier": ("child", "adult", "senior", "student"),
             "shelf_zone": ("A", "B", "C", "D"), "partner_kind": ("school", "museum", "charity")},
        dt=["joined_on", "due_on", "returned_on"], ts=["checked_out_at"],
        txt={"title_text": ("Moon Garden", "the last ferry", "Data & Dragons", "  Salt Roads", "Paper Kites")},
        flag=["is_overdue", "is_reference"], tags={"genre_tags": ("mystery", "scifi", "poetry", "history")},
        nest=("renewals", "renew_no:int:1:3, extra_days:int:7:21, renew_channel:cat:web|desk|app",
              "patron_card", "card_color:cat:green|blue|gold, issued_branch:cat:B1|B2|B3")),
    "museums": dict(
        tables=[("galleries", None), ("artworks", "galleries"), ("exhibitions", "galleries"),
                ("ticket_scans", "exhibitions"), ("conservators", None)],
        num={"insured_value": (1000, 900000, "double"), "visitor_cnt": (0, 3000, "int"), "humidity_rh": (30, 65, "double"),
             "ticket_price": (0, 45, "double"), "created_year": (1500, 2024, "int"), "restoration_hrs": (0, 400, "int")},
        cat={"art_medium": ("oil", "marble", "bronze", "ink", "photo"), "wing_name": ("east", "west", "annex"),
             "ticket_class": ("adult", "child", "member", "free"), "loan_state": ("owned", "on_loan", "borrowed")},
        dt=["acquired_on", "opens_on", "closes_on"], ts=["scanned_at"],
        txt={"artist_credit": ("Unknown", "M. Kittikachorn", "R. Okoye", "L. Weiss", "A. Ferreira")},
        flag=["is_on_display", "needs_climate"], tags={"theme_tags": ("portrait", "landscape", "abstract", "ritual")},
        nest=("provenance", "owner_name:cat:Crown|Estate|Dealer|Temple, held_years:int:1:80, sale_price:double:0:500000",
              "frame_spec", "frame_wood:cat:teak|oak|none, glazing:cat:glass|acrylic|none")),
    "airports": dict(
        tables=[("terminals", None), ("gates", "terminals"), ("baggage_claims", "gates"), ("security_lanes", "terminals"),
                ("concession_sales", "terminals")],
        num={"gate_capacity": (50, 400, "int"), "bag_weight_kg": (1, 32, "double"), "wait_secs": (0, 3600, "int"),
             "lane_throughput": (20, 300, "int"), "sale_amt": (2, 400, "double"), "belt_no": (1, 14, "int")},
        cat={"terminal_code": ("T1", "T2", "T3"), "lane_type": ("standard", "priority", "crew"),
             "bag_state": ("loaded", "delayed", "lost", "claimed"), "vendor_kind": ("food", "duty_free", "books")},
        dt=["opened_on"], ts=["bag_scanned_at", "sold_at"],
        txt={"vendor_label": ("Sky Noodles", "Duty Free Hub", "Gate Cafe", "Jet Books", "Cloud Nine Bar")},
        flag=["is_international", "is_open_24h"], tags={"service_codes": ("WIFI", "LOUNGE", "PRAYER", "SPA")},
        nest=("bag_tags", "tag_code:cat:BKK|NRT|SIN|LHR, leg_no:int:1:3, tag_weight:double:1:32",
              "lane_cfg", "scanner_model:cat:CT-80|XR-2, shift:cat:day|night")),
    "labs": dict(
        tables=[("lab_sites", None), ("specimens", "lab_sites"), ("assay_runs", "specimens"), ("instruments", "lab_sites"),
                ("calibration_checks", "instruments")],
        num={"conc_ng_ml": (0.1, 500, "double"), "run_minutes": (5, 240, "int"), "drift_pct": (0, 5, "double"),
             "well_count": (8, 384, "int"), "reagent_cost": (5, 600, "double"), "cycle_no": (1, 40, "int")},
        cat={"assay_kind": ("PCR", "ELISA", "HPLC", "MS"), "specimen_type": ("blood", "soil", "water", "tissue"),
             "qc_verdict": ("PASS", "FAIL", "RETEST"), "instrument_make": ("Agilent", "Roche", "Thermo", "Bio-Rad")},
        dt=["collected_on", "calibrated_on"], ts=["run_started_at"],
        txt={"sample_note": ("hemolyzed", "ok", "low volume", "  ok ", "frozen twice")},
        flag=["is_urgent", "is_blinded"], tags={"panel_codes": ("CBC", "LIPID", "METAL", "VOC")},
        nest=("readings", "channel:cat:FAM|HEX|ROX, ct_value:double:10:40, replicate:int:1:3",
              "storage", "freezer_id:cat:F1|F2|F3, rack:cat:R1|R2")),
    "shipping": dict(
        tables=[("vessels", None), ("voyages", "vessels"), ("containers", "voyages"), ("port_calls", "voyages"),
                ("crew_members", "vessels"), ("dim_ports", None)],
        num={"teu_capacity": (500, 24000, "int"), "draft_m": (5, 18, "double"), "gross_tonnage": (1000, 220000, "int"),
             "freight_usd": (300, 9000, "double"), "berth_hours": (2, 96, "int"), "reefer_temp": (-30, 15, "double")},
        cat={"vessel_flag": ("PA", "LR", "SG", "MH", "TH"), "container_size": ("20ft", "40ft", "45ft"),
             "voyage_state": ("planned", "sailing", "docked", "closed"), "crew_role": ("master", "officer", "engineer", "rating")},
        dt=["built_on", "departed_on", "arrived_on"], ts=["berthed_at"],
        txt={"vessel_label": ("Ocean Lotus", "Maersk Kobe", "Star of Siam", "Nordic Wave", "Coral Spirit")},
        flag=["is_hazmat", "is_reefer"], tags={"cert_tags": ("ISM", "ISPS", "MARPOL", "SOLAS")},
        nest=("legs", "leg_port:cat:LCB|SIN|HKG|RTM, leg_days:int:1:20, leg_fuel_t:double:10:900",
              "owner_info", "owner_country:cat:DK|CN|JP|TH, fleet_tier:cat:major|mid|small")),
    "transit": dict(
        tables=[("bus_routes", None), ("bus_stops", "bus_routes"), ("stop_visits", "bus_stops"), ("fare_taps", "bus_routes"),
                ("store_kiosks", "bus_stops"), ("depots", None)],
        num={"stop_seq": (1, 40, "int"), "dwell_secs": (5, 300, "int"), "tap_fare": (0.5, 6, "double"),
             "rider_cnt": (0, 120, "int"), "headway_min": (3, 45, "int"), "kiosk_revenue": (0, 3000, "double")},
        cat={"route_kind": ("express", "local", "night"), "stop_label": ("Central", "Market", "Pier", "Uni Gate", "Depot"),
             "tap_media": ("card", "qr", "cash"), "depot_zone": ("north", "south", "river")},
        dt=["route_opened_on"], ts=["visited_at", "tapped_at"],
        txt={"kiosk_owner": ("Mr. Chai", "Lim Family", "Metro Snacks", "Pier Shop", "Uni Co-op")},
        flag=["is_accessible", "has_shelter"], tags={"line_tags": ("BRT", "feeder", "loop", "school")},
        nest=("timetable", "slot_no:int:1:12, depart_min:int:0:59, crowd_level:cat:low|mid|high",
              "stop_geo", "ward:cat:W1|W2|W3, side:cat:N|S")),
    "hospitality": dict(
        tables=[("hotels", None), ("room_types", "hotels"), ("guest_stays", "room_types"), ("minibar_charges", "guest_stays"),
                ("housekeeping_tasks", "hotels")],
        num={"nightly_rate": (30, 900, "double"), "nights": (1, 21, "int"), "star_rating": (1, 5, "int"),
             "charge_amt": (1, 200, "double"), "task_minutes": (10, 120, "int"), "occupancy_pct": (10, 100, "double")},
        cat={"board_basis": ("RO", "BB", "HB", "FB"), "stay_channel": ("direct", "ota", "corporate", "walk_in"),
             "task_kind": ("clean", "turndown", "repair", "inspect"), "hotel_chain": ("Lotus", "Orchid", "Indie")},
        dt=["checkin_day", "checkout_day"], ts=["charged_at"],
        txt={"guest_alias": ("Mr. Sato", "Ms. Brown", "Dr. Nwosu", "Mrs. Silva", "Khun Nok")},
        flag=["is_vip", "late_checkout"], tags={"perk_codes": ("SPA", "BRKF", "UPGR", "PARK")},
        nest=("folio_lines", "line_kind:cat:room|food|spa|tax, line_amt:double:1:600, line_qty:int:1:5",
              "loyalty", "tier_name:cat:silver|gold|platinum, points_band:cat:low|mid|high")),
    "restaurants": dict(
        tables=[("eateries", None), ("menu_dishes", "eateries"), ("table_tickets", "eateries"),
                ("ticket_lines", "table_tickets"), ("tbl_suppliers_food", None)],
        num={"dish_price": (2, 80, "double"), "cover_cnt": (1, 12, "int"), "prep_minutes": (2, 60, "int"),
             "line_qty": (1, 8, "int"), "tip_amt": (0, 50, "double"), "spice_level": (0, 5, "int")},
        cat={"cuisine_kind": ("thai", "italian", "japanese", "mexican", "indian"), "course_kind": ("starter", "main", "dessert"),
             "ticket_state": ("open", "paid", "void"), "seat_zone": ("patio", "bar", "hall")},
        dt=["opened_day"], ts=["ordered_at", "served_at"],
        txt={"dish_title": ("Pad Kra Pao", "margherita", "Tonkotsu Ramen", "  Tacos al Pastor", "Butter Chicken")},
        flag=["is_vegan", "is_takeaway"], tags={"allergen_tags": ("nuts", "gluten", "dairy", "shellfish")},
        nest=("modifiers", "mod_code:cat:XL|NOSPICE|EXTRA|SIDE, mod_price:double:0:6, mod_qty:int:1:3",
              "kitchen", "station:cat:wok|grill|cold, chef_shift:cat:am|pm")),
    "fitness": dict(
        tables=[("gyms", None), ("members_fit", "gyms"), ("workout_sessions", "members_fit"), ("class_slots", "gyms"),
                ("class_signups", "class_slots")],
        num={"heart_rate_avg": (60, 190, "int"), "kcal_burned": (50, 1500, "double"), "session_mins": (10, 150, "int"),
             "slot_capacity": (5, 40, "int"), "membership_fee": (10, 150, "double"), "body_fat_pct": (5, 45, "double")},
        cat={"class_kind": ("yoga", "spin", "hiit", "boxing", "pilates"), "member_plan": ("basic", "plus", "elite"),
             "signup_state": ("booked", "attended", "no_show", "cancelled"), "gym_area": ("cardio", "free_weights", "studio")},
        dt=["member_since", "slot_day"], ts=["session_started_at"],
        txt={"coach_name": ("Coach Bee", "Coach Max", "Coach Ana", "Coach Ike", "Coach Rin")},
        flag=["has_locker", "is_trial"], tags={"goal_tags": ("strength", "cardio", "mobility", "weight_loss")},
        nest=("sets_done", "exercise:cat:squat|bench|row|deadlift, reps:int:1:20, load_kg:double:5:200",
              "wearable", "brand:cat:fitbit|garmin|apple, sync_mode:cat:auto|manual")),
    "manufacturing": dict(
        tables=[("factories", None), ("production_lines", "factories"), ("work_orders", "production_lines"),
                ("defect_reports", "work_orders"), ("machine_assets", "production_lines"), ("dim_shifts", None)],
        num={"units_built": (0, 5000, "int"), "scrap_rate": (0, 0.2, "double"), "cycle_secs": (5, 600, "int"),
             "oee_pct": (30, 98, "double"), "defect_cnt": (0, 40, "int"), "torque_nm": (5, 400, "double")},
        cat={"line_type": ("assembly", "paint", "weld", "pack"), "order_priority": ("P1", "P2", "P3"),
             "defect_kind": ("scratch", "dent", "misalign", "crack"), "shift_code": ("A", "B", "C")},
        dt=["started_on", "due_by"], ts=["reported_at"],
        txt={"machine_tag": ("PRS-01", "WLD-7", "CNC-22", "PNT-3", "PKG-9")},
        flag=["is_rework", "is_automated"], tags={"tool_codes": ("TQ", "LZ", "DR", "GR")},
        nest=("stations", "station_id:cat:S1|S2|S3|S4, station_secs:int:5:300, pass_rate:double:0.7:1",
              "bom_ref", "bom_rev:cat:R1|R2|R3, material:cat:steel|alu|abs")),
    "retail_ops": dict(
        tables=[("outlets", None), ("shelf_audits", "outlets"), ("restock_orders", "outlets"), ("promo_runs", "outlets"),
                ("store_visits", "outlets")],
        num={"restock_qty": (0, 500, "int"), "revenue_share": (0, 0.5, "double"), "facings": (1, 24, "int"),
             "visit_secs": (30, 3600, "int"), "promo_lift_pct": (-10, 60, "double"), "shrink_units": (0, 30, "int")},
        cat={"outlet_format": ("hyper", "super", "mini", "kiosk"), "audit_result": ("ok", "gap", "misplaced"),
             "promo_mech": ("bogo", "pct_off", "bundle"), "visit_purpose": ("audit", "merch", "training")},
        dt=["restocked_on", "promo_start_day"], ts=["visited_at"],
        txt={"rep_name": ("Nida K.", "Tom R.", "Aiko S.", "Ravi P.", "Elena M.")},
        flag=["is_franchise", "out_of_stock"], tags={"planogram_tags": ("eye_level", "end_cap", "checkout", "cooler")},
        nest=("audit_lines", "sku_code:cat:SK1|SK2|SK3|SK4, shelf_qty:int:0:40, price_ok:bool",
              "outlet_geo", "metro:cat:BKK|CNX|HDY, footfall_band:cat:low|mid|high")),
    "banking_ops": dict(
        tables=[("bank_branches", None), ("tellers", "bank_branches"), ("cash_drawers", "tellers"), ("loan_apps", "bank_branches"),
                ("atm_units", "bank_branches")],
        num={"drawer_balance": (0, 50000, "double"), "loan_amt_req": (1000, 500000, "double"), "credit_band": (300, 850, "int"),
             "atm_cash_level": (0, 100, "int"), "txn_cnt": (0, 900, "int"), "apr_pct": (1, 30, "double")},
        cat={"app_state": ("submitted", "approved", "declined", "withdrawn"), "loan_purpose": ("home", "car", "edu", "biz"),
             "atm_model": ("NCR", "Diebold", "Hyosung"), "teller_grade": ("T1", "T2", "T3")},
        dt=["applied_on", "decided_on"], ts=["drawer_closed_at"],
        txt={"applicant_ref": ("APP-1001", "APP-2210", "APP-3302", "APP-0457", "APP-9981")},
        flag=["is_cosigned", "needs_cash"], tags={"doc_codes": ("ID", "PAYSLIP", "BANKSTMT", "TAX")},
        nest=("denominations", "note_value:int:20:1000, note_cnt:int:0:200, bundle:cat:A|B",
              "branch_hours", "opens:cat:08|09|10, closes:cat:16|17|18")),
    "events": dict(
        tables=[("venues_ev", None), ("concerts", "venues_ev"), ("ticket_sales", "concerts"), ("performers", None),
                ("setlists", "concerts")],
        num={"ticket_price": (10, 400, "double"), "seats_sold": (0, 20000, "int"), "encore_cnt": (0, 4, "int"),
             "set_minutes": (20, 180, "int"), "merch_revenue": (0, 90000, "double"), "stage_width_m": (6, 40, "double")},
        cat={"genre_code": ("rock", "pop", "jazz", "edm", "luk_thung"), "tier_section": ("floor", "lower", "upper", "vip"),
             "sale_channel": ("box_office", "app", "reseller"), "show_status": ("scheduled", "sold_out", "cancelled")},
        dt=["show_day", "announced_on"], ts=["sold_at"],
        txt={"act_name": ("The Mangos", "DJ Kraiwit", "Blue Lanterns", "Nova Kid", "Silk Road Trio")},
        flag=["is_festival", "is_outdoor"], tags={"sponsor_tags": ("beer", "telco", "bank", "auto")},
        nest=("songs", "song_title:cat:Opener|Hit One|Ballad|Closer, song_secs:int:120:480, is_cover:bool",
              "promoter", "promoter_name:cat:LiveNation|LocalCo|Indie, region:cat:Asia|EU|US")),
    "logistics_wh": dict(
        tables=[("warehouses_v", None), ("pick_tasks", "warehouses_v"), ("pallets", "warehouses_v"), ("dock_slots", "warehouses_v"),
                ("forklift_logs", "warehouses_v")],
        num={"pick_lines": (1, 60, "int"), "pallet_kg": (50, 1200, "double"), "slot_minutes": (15, 120, "int"),
             "lift_hours": (0, 12, "double"), "aisle_no": (1, 40, "int"), "pick_secs": (5, 900, "int")},
        cat={"pick_wave": ("W1", "W2", "W3", "W4"), "pallet_state": ("inbound", "stored", "staged", "shipped"),
             "dock_side": ("north", "south"), "lift_model": ("Toyota", "Linde", "Jungheinrich")},
        dt=["received_on"], ts=["picked_at", "slot_start_at"],
        txt={"picker_code": ("PK-01", "PK-02", "PK-17", "PK-33", "PK-40")},
        flag=["is_fragile", "is_cold_chain"], tags={"handling_tags": ("stack", "upright", "dry", "fragile")},
        nest=("pick_lines_list", "bin_code:cat:A01|B07|C12|D03, picked_qty:int:1:30, short_pick:bool",
              "dock_info", "door_no:cat:D1|D2|D3, carrier_code:cat:KRY|FLS|DHL")),
    "healthtech": dict(
        tables=[("clinics_v", None), ("care_plans", "clinics_v"), ("vital_logs", "care_plans"), ("pharmacy_fills", "care_plans"),
                ("nurse_rosters", "clinics_v")],
        num={"pulse_bpm": (40, 160, "int"), "systolic_mm": (90, 190, "int"), "dose_mg": (5, 1000, "double"),
             "refill_cnt": (0, 6, "int"), "shift_hours": (4, 12, "double"), "bmi_value": (15, 45, "double")},
        cat={"plan_kind": ("chronic", "post_op", "prenatal", "rehab"), "fill_state": ("filled", "partial", "rejected"),
             "ward_code": ("W1", "W2", "W3", "ICU"), "nurse_grade": ("RN", "LPN", "NP")},
        dt=["plan_start_day", "filled_on"], ts=["logged_at"],
        txt={"drug_label": ("Metformin 500", "amlodipine 5", "Atorvastatin 20", "  Losartan 50", "Insulin G")},
        flag=["is_telehealth", "needs_followup"], tags={"alert_tags": ("fall_risk", "allergy", "dnr", "isolation")},
        nest=("med_doses", "drug_code:cat:MET|AML|ATV|LOS, dose_units:double:0.5:4, taken:bool",
              "care_team", "lead_role:cat:gp|cardio|endo, team_size:cat:small|large")),
    "media": dict(
        tables=[("channels_tv", None), ("broadcasts", "channels_tv"), ("ad_spots", "broadcasts"), ("viewer_panels", None),
                ("rating_samples", "broadcasts")],
        num={"spot_secs": (5, 60, "int"), "spot_rate": (100, 20000, "double"), "rating_pts": (0, 25, "double"),
             "panel_size": (100, 5000, "int"), "reach_k": (1, 900, "int"), "share_pct": (0, 60, "double")},
        cat={"daypart": ("morning", "daytime", "prime", "late"), "spot_kind": ("ad", "promo", "psa"),
             "channel_genre": ("news", "drama", "sport", "kids"), "panel_region": ("BKK", "UPC", "SOUTH")},
        dt=["aired_on"], ts=["spot_aired_at"],
        txt={"program_title": ("Morning Brew", "Lakorn Night", "Goal Rush", "  Kids Corner", "Late Talk")},
        flag=["is_live", "is_rerun"], tags={"audience_tags": ("teen", "adult", "family", "senior")},
        nest=("breaks", "break_no:int:1:6, break_secs:int:30:240, sponsor_cat:cat:fmcg|auto|telco",
              "signal", "format:cat:HD|SD|UHD, carrier:cat:sat|cable|ott")),
    "agri_market": dict(
        tables=[("trading_posts", None), ("commodity_lots", "trading_posts"), ("bids", "commodity_lots"), ("traders", None),
                ("price_ticks", "commodity_lots")],
        num={"lot_tonnes": (1, 500, "double"), "bid_price": (100, 3000, "double"), "moisture_pct": (8, 25, "double"),
             "tick_volume": (0, 900, "int"), "lot_grade_no": (1, 5, "int"), "spread_bps": (0, 300, "int")},
        cat={"commodity": ("rice", "sugar", "rubber", "palm_oil", "coffee"), "bid_state": ("open", "won", "lost"),
             "post_region": ("north", "isan", "south", "east"), "trader_type": ("mill", "exporter", "broker")},
        dt=["lot_listed_on"], ts=["bid_at", "ticked_at"],
        txt={"trader_alias": ("Siam Grain", "Rubber King", "PalmCo", "Bean & Co", "Golden Field")},
        flag=["is_certified", "is_export"], tags={"quality_tags": ("fairtrade", "organic", "gi", "halal")},
        nest=("lab_checks", "check_kind:cat:aflatoxin|moisture|purity, check_val:double:0:30, passed:bool",
              "source_area", "province:cat:SUR|KKN|SRT, farm_size:cat:small|mid|large")),
    "water_utility": dict(
        tables=[("pump_stations", None), ("water_meters", "pump_stations"), ("usage_reads", "water_meters"),
                ("pipe_bursts", "pump_stations"), ("dim_tariffs_water", None)],
        num={"flow_lpm": (0, 3000, "double"), "usage_m3": (0, 90, "double"), "pressure_bar": (0.5, 8, "double"),
             "burst_litres": (100, 90000, "int"), "meter_age_yrs": (0, 30, "int"), "chlorine_ppm": (0.1, 4, "double")},
        cat={"meter_kind": ("analog", "smart", "ultrasonic"), "burst_cause": ("age", "pressure", "digging", "freeze"),
             "station_tier": ("primary", "booster", "reserve"), "tariff_band": ("house", "shop", "factory")},
        dt=["meter_fitted_on"], ts=["read_at", "burst_at"],
        txt={"street_ref": ("Soi 3", "Main Rd", "Canal Ln", "Temple St", "Market Alley")},
        flag=["is_leaking", "is_estimated"], tags={"zone_tags": ("dma1", "dma2", "dma3", "dma4")},
        nest=("valve_ops", "valve_id:cat:V1|V2|V3, turns:int:1:12, opened:bool",
              "meter_spec", "maker:cat:Itron|Kamstrup|Sensus, size_mm:cat:15|20|25")),
    "aviation_mro": dict(
        tables=[("aircraft_fleet", None), ("maintenance_jobs", "aircraft_fleet"), ("part_swaps", "maintenance_jobs"),
                ("technicians", None), ("engine_cycles", "aircraft_fleet")],
        num={"flight_hours": (100, 60000, "int"), "job_hours": (1, 300, "double"), "part_cost": (50, 250000, "double"),
             "cycle_cnt": (0, 30000, "int"), "egt_margin_c": (0, 80, "double"), "seat_cfg": (90, 400, "int")},
        cat={"airframe": ("A320", "A350", "B737", "B787", "ATR72"), "check_type": ("A", "B", "C", "D"),
             "job_status": ("open", "deferred", "closed"), "tech_license": ("B1", "B2", "C")},
        dt=["delivered_on", "job_opened_on"], ts=["swapped_at"],
        txt={"tail_no": ("HS-TKA", "HS-PGX", "9V-SMF", "JA-801A", "VH-OQA")},
        flag=["is_aog", "is_leased"], tags={"ad_codes": ("AD-2021", "AD-2023", "SB-55", "SB-70")},
        nest=("findings", "zone:cat:wing|gear|engine|cabin, finding_sev:int:1:5, labor_hrs:double:0.5:40",
              "lessor", "lessor_name:cat:AerCap|BOC|SMBC, term:cat:short|long")),
    "ecommerce_v": dict(
        tables=[("storefronts", None), ("shopper_accounts", None), ("baskets", "shopper_accounts"), ("basket_lines", "baskets"),
                ("parcel_drops", "baskets"), ("fact_reviews", "storefronts")],
        num={"line_qty": (1, 9, "int"), "line_net": (1, 900, "double"), "parcel_kg": (0.1, 30, "double"),
             "review_stars": (1, 5, "int"), "voucher_amt": (0, 60, "double"), "basket_total": (5, 2500, "double")},
        cat={"pay_mode": ("cod", "card", "wallet", "bnpl"), "drop_status": ("queued", "out", "delivered", "failed"),
             "store_tier": ("mall", "preferred", "standard"), "basket_state": ("open", "checked_out", "abandoned")},
        dt=["account_opened_on"], ts=["checked_out_at", "dropped_at"],
        txt={"shopper_handle": ("mintmint", "ploy_shop", "deal_hunter", "  bigboss", "kawaii_k")},
        flag=["is_flash_sale", "is_gift"], tags={"coupon_tags": ("FREESHIP", "NEW10", "MEGA", "PAYDAY")},
        nest=("basket_items", "item_sku:cat:A100|B200|C300|D400, item_qty:int:1:6, item_net:double:1:400",
              "ship_addr", "province:cat:BKK|NBI|CBI, addr_kind:cat:home|office|locker")),
    "construction": dict(
        tables=[("build_projects", None), ("site_crews", "build_projects"), ("material_orders", "build_projects"),
                ("safety_incidents", "build_projects"), ("subcontractors", None)],
        num={"budget_thb": (1e5, 9e7, "double"), "crew_size": (2, 80, "int"), "concrete_m3": (0, 900, "double"),
             "lost_days": (0, 30, "int"), "progress_pct": (0, 100, "double"), "floor_cnt": (1, 70, "int")},
        cat={"project_kind": ("tower", "bridge", "mall", "road"), "incident_class": ("near_miss", "minor", "major"),
             "material_kind": ("steel", "cement", "glass", "timber"), "permit_state": ("pending", "granted", "expired")},
        dt=["broke_ground_on", "target_done_on"], ts=["incident_at"],
        txt={"foreman_name": ("Boonmee", "Carlos", "Wei Jun", "Sipho", "Petra")},
        flag=["is_government", "has_crane"], tags={"ppe_tags": ("helmet", "harness", "boots", "gloves")},
        nest=("milestones", "stage:cat:piling|frame|mep|finish, stage_pct:double:0:100, late_days:int:0:60",
              "site_info", "soil:cat:clay|rock|sand, access:cat:road|river")),
    "pets": dict(
        tables=[("pet_owners", None), ("pets_reg", "pet_owners"), ("vet_visits", "pets_reg"), ("grooming_jobs", "pets_reg"),
                ("kennels", None)],
        num={"pet_weight_kg": (0.5, 70, "double"), "visit_cost": (10, 900, "double"), "pet_age_yrs": (0, 20, "int"),
             "groom_minutes": (20, 180, "int"), "kennel_nights": (1, 30, "int"), "vaccine_cnt": (0, 8, "int")},
        cat={"species": ("dog", "cat", "rabbit", "bird"), "visit_reason": ("checkup", "vaccine", "injury", "dental"),
             "coat_kind": ("short", "long", "curly"), "kennel_size": ("S", "M", "L")},
        dt=["adopted_on", "visit_day"], ts=["groomed_at"],
        txt={"pet_nickname": ("Mochi", "Bao", "Captain", "  Luna", "Tiger")},
        flag=["is_neutered", "is_microchipped"], tags={"diet_tags": ("grain_free", "senior", "kitten", "renal")},
        nest=("vaccines", "vaccine:cat:rabies|dhpp|fvrcp, dose_no:int:1:3, booster_due:bool",
              "owner_contact", "pref:cat:line|sms|call, district:cat:D1|D2|D3")),
    "cloud_ops": dict(
        tables=[("clusters", None), ("vm_instances", "clusters"), ("cpu_samples", "vm_instances"), ("deploy_jobs", "clusters"),
                ("incident_pages", "clusters"), ("dim_teams", None)],
        num={"vcpu_cnt": (1, 96, "int"), "cpu_util_pct": (0, 100, "double"), "mem_gib": (1, 768, "int"),
             "deploy_secs": (10, 3600, "int"), "cost_per_hr": (0.01, 30, "double"), "page_ack_min": (0, 120, "int")},
        cat={"cloud_region": ("ap-se-1", "us-east-1", "eu-west-1"), "deploy_result": ("success", "rollback", "failed"),
             "vm_family": ("gp", "mem", "cpu", "gpu"), "page_sev": ("sev1", "sev2", "sev3")},
        dt=["provisioned_on"], ts=["sampled_at", "paged_at"],
        txt={"hostname_txt": ("api-01", "db-primary", "cache-3", "  worker-7", "edge-2")},
        flag=["is_spot", "is_prod"], tags={"label_tags": ("web", "batch", "ml", "infra")},
        nest=("containers_run", "image:cat:nginx|redis|app|sidecar, restarts:int:0:9, mem_mib:double:16:4096",
              "owner_team", "team:cat:core|data|edge, oncall:cat:yes|no")),
    "tourism": dict(
        tables=[("tour_operators", None), ("tour_packages", "tour_operators"), ("tour_bookings_v", "tour_packages"),
                ("guide_staff", "tour_operators"), ("review_posts", "tour_packages")],
        num={"pkg_price": (20, 3000, "double"), "group_size": (1, 40, "int"), "tour_days": (1, 14, "int"),
             "guide_rating": (1, 5, "double"), "deposit_amt": (0, 900, "double"), "review_likes": (0, 500, "int")},
        cat={"tour_kind": ("island", "trek", "temple", "food", "dive"), "pkg_season": ("high", "shoulder", "low"),
             "pay_state": ("deposit", "paid", "refunded"), "guide_lang": ("th", "en", "zh", "ja")},
        dt=["departs_on", "posted_on"], ts=["reserved_at"],
        txt={"pkg_title": ("Phi Phi Day Trip", "doi suthep trek", "Ayutthaya Ruins", "  Night Market Eats", "Similan Dive")},
        flag=["includes_meals", "is_private"], tags={"activity_tags": ("snorkel", "hike", "cook", "kayak")},
        nest=("itinerary", "day_no:int:1:10, stop_place:cat:beach|temple|market|cave, km:double:0:120",
              "operator_lic", "lic_class:cat:A|B, province:cat:PKT|CNX|KBI")),
    "auto_dealer": dict(
        tables=[("showrooms", None), ("car_stock", "showrooms"), ("test_drives", "car_stock"), ("finance_deals", "car_stock"),
                ("service_tickets", "showrooms")],
        num={"msrp_thb": (4e5, 6e6, "double"), "odometer_km": (0, 200000, "int"), "drive_minutes": (5, 90, "int"),
             "down_payment": (0, 2e6, "double"), "term_months": (12, 84, "int"), "labor_hrs": (0.5, 20, "double")},
        cat={"body_style": ("sedan", "suv", "pickup", "hatch"), "drive_outcome": ("bought", "thinking", "declined"),
             "deal_state": ("quoted", "signed", "funded"), "fuel_kind": ("petrol", "diesel", "ev", "hybrid")},
        dt=["arrived_on", "signed_on"], ts=["drive_started_at"],
        txt={"vin_tail": ("A12345", "B77810", "C00921", "D55102", "E31337")},
        flag=["is_certified_used", "is_fleet"], tags={"option_tags": ("sunroof", "adas", "leather", "tow")},
        nest=("options_fitted", "opt_code:cat:SR|ADAS|LTH|TOW, opt_price:double:5000:90000, factory:bool",
              "trade_in", "trade_make:cat:Toyota|Honda|Isuzu, trade_band:cat:low|mid|high")),
    "charity": dict(
        tables=[("campaigns_ch", None), ("donors", None), ("pledges", "donors"), ("grant_awards", "campaigns_ch"),
                ("volunteer_shifts", "campaigns_ch")],
        num={"pledge_amt": (5, 50000, "double"), "goal_amt": (1000, 2e6, "double"), "grant_amt": (500, 900000, "double"),
             "volunteer_hrs": (1, 12, "double"), "donor_age": (18, 90, "int"), "gift_cnt": (1, 60, "int")},
        cat={"cause_area": ("health", "education", "climate", "animals"), "pledge_freq": ("once", "monthly", "yearly"),
             "grant_stage": ("applied", "awarded", "reported"), "donor_segment": ("major", "mid", "grassroots")},
        dt=["pledged_on", "awarded_on"], ts=["shift_start_at"],
        txt={"donor_alias": ("Anon", "The Lims", "J. Okafor", "  Rattanakorn F.", "M. Dubois")},
        flag=["is_recurring", "gift_aid"], tags={"channel_tags": ("email", "event", "street", "online")},
        nest=("allocations", "fund:cat:general|food|school|clinic, share_pct:double:0:100, restricted:bool",
              "donor_pref", "contact:cat:email|post|none, anon:cat:yes|no")),
    "space": dict(
        tables=[("launch_sites", None), ("rockets", None), ("missions", "rockets"), ("payload_items", "missions"),
                ("telemetry_frames", "missions")],
        num={"payload_kg": (1, 22000, "double"), "apogee_km": (100, 36000, "double"), "burn_secs": (10, 600, "int"),
             "frame_seq": (1, 9999, "int"), "thrust_kn": (100, 8000, "double"), "stage_cnt": (1, 3, "int")},
        cat={"orbit_class": ("LEO", "MEO", "GEO", "SSO"), "mission_state": ("planned", "success", "partial", "lost"),
             "payload_kind": ("sat", "cargo", "probe", "crew"), "site_country": ("US", "FR", "IN", "JP")},
        dt=["launched_on"], ts=["frame_at"],
        txt={"mission_name": ("Kestrel-1", "Aurora", "Deep Wren", "  Lotus Sat", "Atlas Ridge")},
        flag=["is_reusable", "is_crewed"], tags={"agency_tags": ("NASA", "ESA", "ISRO", "JAXA")},
        nest=("stages_flown", "stage_no:int:1:3, stage_burn:int:30:400, recovered:bool",
              "orbit_params", "incl_band:cat:low|mid|polar, ecc_band:cat:circ|ell")),
}

# never in training: their pairs are the schema-shift eval set (eval/schema_shift_tables.py)
HELDOUT_DOMAINS = ("pets", "space", "charity", "auto_dealer", "water_utility")

# identifiers of the hold-out / real-world query tables that no training table uses; they must not appear here
FORBIDDEN = {
    "patients", "doctors", "appointments", "flights", "bookings", "vehicle_trips", "full_name_x", "doctor_name",
    "specialty", "hospital", "years_experience", "insurance_type", "is_smoker", "appt_date", "appt_type",
    "wait_minutes", "airline", "origin", "destination", "departure_time", "distance_km", "base_fare",
    "passenger_name", "seat_class", "booked_at", "is_cancelled", "loyalty_tier", "trip_start", "stops", "route",
    "telemetry_json", "extras", "fare_components", "surcharge_rates", "stop_name", "wait_min", "fare",
    "raw_events", "campaigns", "blacklist", "line_items", "eligible_warehouses", "fulfillment_flags", "sku",
    "warehouse_id", "origin_country", "batch_id", "acquisition_cost", "warehouse_sla", "dispatch_threshold_hours",
    "sensor_telemetry", "calibration_registry", "operational_incidents", "device_metadata", "trade_stream",
    "cleared_settlement_batches", "raw_status_mask", "firmware_id", "scale_multiplier", "calibration_offset",
    "incident_severity", "incident_code", "region_code", "raw_symbol", "execution_details", "fee_tier_rates",
    "applicable_multipliers", "burst_island_id", "campaign_type", "channel_code", "flagged_date", "order_timestamp",
    "shipping_address", "postal_code", "unit_price_x", "metric_name", "effective_from_x", "trade_timestamp",
}
RESERVED = {"date", "time", "timestamp", "year", "month", "day", "hour", "order", "group", "limit", "rank", "row",
            "rows", "range", "interval", "table", "user", "end", "current", "value", "values", "position", "level"}

N_ROWS = 40
_SPARK_T = {"int": "INT", "double": "DOUBLE", "cat": "STRING", "bool": "BOOLEAN"}


def singular(name: str) -> str:
    for pre in ("dim_", "fact_", "tbl_"):
        if name.startswith(pre):
            name = name[len(pre):]
    if name.endswith("ies"):
        return name[:-3] + "y"
    if name.endswith("ses") or name.endswith("xes"):
        return name[:-2]
    if name.endswith("s") and not name.endswith("ss"):
        return name[:-1]
    return name


def _fields(spec: str) -> list[tuple]:
    """'name:kind[:args]' items -> [(name, kind, args)]"""
    out = []
    for part in spec.split(","):
        bits = part.strip().split(":")
        name, kind, rest = bits[0], bits[1], bits[2:]
        if kind == "cat":
            out.append((name, "cat", tuple(rest[0].split("|"))))
        elif kind in ("int", "double"):
            out.append((name, kind, (float(rest[0]), float(rest[1]))))
        else:
            out.append((name, "bool", ()))
    return out


def struct_type(fields: list[tuple]) -> str:
    return "STRUCT<" + ", ".join(f"{n}: {_SPARK_T[k]}" for n, k, _ in fields) + ">"


def _field_value(rng, kind, args):
    if kind == "cat":
        return rng.choice(args)
    if kind == "int":
        return rng.randint(int(args[0]), int(args[1]))
    if kind == "double":
        return round(rng.uniform(args[0], args[1]), 2)
    return rng.random() < 0.5


def _nested_rows(table: Table, arr_fields, st_fields):
    """rows for a nested table: plain columns as schemas.make_rows would fill them + the array / struct columns"""
    from schemas import _value

    def rows(seed):
        rng = random.Random(f"{seed}-{table.name}")
        out = []
        for i in range(1, N_ROWS + 1):
            row = []
            for c in table.columns:
                if c.kind == "array_struct":
                    n = rng.choice([0, 1, 2, 2, 3, 4])
                    row.append([tuple(_field_value(rng, k, a) for _, k, a in arr_fields) for _ in range(n)])
                elif c.kind == "struct":
                    row.append(None if rng.random() < 0.06 else tuple(_field_value(rng, k, a) for _, k, a in st_fields))
                else:
                    row.append(_value(c, rng, i))
            out.append(tuple(row))
        return out
    return rows


# per-table naming variants: the same concept is abbreviated in one table and spelled out in another
TOKEN_VARIANTS = {"cnt": "count", "count": "cnt", "amt": "amount", "amount": "amt", "pct": "percent",
                  "percent": "pct", "secs": "seconds", "seconds": "secs", "mins": "minutes", "minutes": "mins",
                  "min": "minutes", "qty": "quantity", "quantity": "qty", "no": "number", "hrs": "hours",
                  "hours": "hrs", "kg": "kilos", "km": "kms", "on": "date", "at": "ts", "is": "flag_is",
                  "has": "flag_has", "price": "px", "rate": "rt", "value": "val", "avg": "mean", "code": "cd",
                  "kind": "type", "type": "kind", "state": "status", "status": "state", "label": "name",
                  "name": "label", "level": "lvl", "total": "tot", "usd": "dollars", "thb": "baht"}


def variant(name: str, rng) -> str:
    toks = name.split("_")
    out = [TOKEN_VARIANTS.get(t, t) if rng.random() < 0.7 else t for t in toks]
    v = "_".join(out)
    return v if v not in RESERVED and v not in FORBIDDEN else name


def _table_cols(rng, dom: dict, name: str, parent: str | None, used_fk: dict) -> list[Column]:
    cols = [Column("id", "id", lo=1, hi=N_ROWS, nullable=False)]
    if parent:
        fk = rng.choice([f"{singular(parent)}_id"] * 4 + [f"{singular(parent)}_ref", f"{singular(parent)}_no"])
        cols.append(Column(fk, "fk", lo=1, hi=N_ROWS, ref=parent, nullable=rng.random() < 0.4))
    cats = rng.sample(sorted(dom["cat"]), min(len(dom["cat"]), rng.choice([1, 2, 2])))
    for j, c in enumerate(cats):
        cols.append(Column(c, "cat", pool=dom["cat"][c], nullable=j > 0))
    for n in rng.sample(sorted(dom["num"]), rng.choice([1, 2, 2, 3])):
        lo, hi, kind = dom["num"][n]
        cols.append(Column(n, kind, lo=lo, hi=hi, nullable=rng.random() < 0.6))
    if rng.random() < 0.7:
        cols.append(Column(rng.choice(dom["dt"]), "date", nullable=rng.random() < 0.5))
    else:
        cols.append(Column(rng.choice(dom["ts"]), "ts"))
    if rng.random() < 0.45:
        n = rng.choice(sorted(dom["txt"]))
        cols.append(Column(n, "str", pool=dom["txt"][n]))
    if rng.random() < 0.4:
        cols.append(Column(rng.choice(dom["flag"]), "bool"))
    if rng.random() < 0.15:
        n = rng.choice(sorted(dom["tags"]))
        cols.append(Column(n, "array", pool=dom["tags"][n]))
    if rng.random() < 0.45:                 # this table spells its columns differently (fk / id stay)
        from dataclasses import replace
        cols = [c if c.kind in ("id", "fk") else replace(c, name=variant(c.name, rng)) for c in cols]
    seen, out = set(), []
    for c in cols:
        if c.name not in seen:
            seen.add(c.name)
            out.append(c)
    return out


def build_catalog(seed: int = 15) -> dict:
    """{domain: {"tables": [Table, ...], "nested": Table}} — deterministic for a seed"""
    rng = random.Random(seed)
    cat = {}
    for dname, dom in DOMAINS.items():
        tables, used_fk = [], {}
        for name, parent in dom["tables"]:
            tables.append(Table(name, tuple(_table_cols(rng, dom, name, parent, used_fk))))
        arr, arr_spec, st, st_spec = dom["nest"]
        af, sf = _fields(arr_spec), _fields(st_spec)
        base = tables[0]
        ncols = [c for c in base.columns if c.kind not in ("fk", "array")][:5]
        nname = f"{singular(base.name)}_{arr}_log" if rng.random() < 0.5 else f"{base.name}_nested"
        ncols += [Column(arr, "array_struct", dtype=f"ARRAY<{struct_type(af)}>", nullable=False),
                  Column(st, "struct", dtype=struct_type(sf))]
        nt = Table(nname, tuple(ncols))
        nt = Table(nname, tuple(ncols), rows=_nested_rows(nt, af, sf))
        cat[dname] = {"tables": tables, "nested": nt, "arr_fields": af, "st_fields": sf, "arr": arr, "st": st}
    _check(cat)
    return cat


def _check(cat: dict) -> None:
    from schemas import EXTRA_TABLES, TABLES
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "complex_v8"))
    import importlib
    cx = importlib.import_module("catalog")  # complex_v8/catalog.py
    train_tables = {t.name for t in TABLES + EXTRA_TABLES} | {t.name for t in cx.TABLES}
    names, errs = [], []
    for d in cat.values():
        for t in d["tables"] + [d["nested"]]:
            names.append(t.name)
            if t.name in train_tables:
                errs.append(f"table {t.name} is a training table")
            errs += [f"{t.name}.{c.name} is forbidden" for c in t.columns if c.name in FORBIDDEN | RESERVED]
        errs += [f"field {f} is forbidden" for f, _, _ in d["arr_fields"] + d["st_fields"] if f in FORBIDDEN | RESERVED]
    assert not errs, "; ".join(sorted(set(errs)))
    assert len(names) == len(set(names)), "duplicate table names"


CATALOG = build_catalog()


def train_domain_tables() -> list[Table]:
    return [t for d, v in CATALOG.items() if d not in HELDOUT_DOMAINS for t in v["tables"]]


def heldout_domain_tables() -> list[Table]:
    return [t for d, v in CATALOG.items() if d in HELDOUT_DOMAINS for t in v["tables"]]


def all_tables() -> list[Table]:
    return [t for v in CATALOG.values() for t in v["tables"] + [v["nested"]]]


def make_dataframes(spark, seed: int = 7, tables=None) -> dict:
    """{"df_<table>": DataFrame} for the catalog (all domains by default); each table is also a temp view"""
    from schemas import make_rows
    frames = {}
    for t in tables or all_tables():
        df = spark.createDataFrame(make_rows(t, seed), t.ddl)
        df.createOrReplaceTempView(t.name)
        frames[f"df_{t.name}"] = df
    return frames


if __name__ == "__main__":
    tabs = all_tables()
    cols = {c.name for t in tabs for c in t.columns}
    print(f"{len(CATALOG)} domains, {len(tabs)} tables ({len(heldout_domain_tables())} flat held-out), "
          f"{len(cols)} distinct column names")
