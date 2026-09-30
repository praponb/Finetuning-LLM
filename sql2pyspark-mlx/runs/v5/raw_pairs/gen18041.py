from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city")
w_hist = Window.partitionBy("country").orderBy("email_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w_part = Window.orderBy(F.col("seq_no").desc(), "id")
w5 = Window.partitionBy("country").orderBy("seq_no", "id")
w6 = Window.partitionBy("city").orderBy(F.col("seq_no_bucket").desc())
w7 = Window.partitionBy("city").orderBy("seq_no_bucket", "id")

base = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-03-21")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "is_active",
        "age",
        F.length("email").alias("email_length"),
    )
)

metrics = (
    base
    .filter(F.col("country") != "TH")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        F.avg("email_length").over(w_running).alias("city_avg_email_length"),
        F.first("city", ignorenulls=True).over(w_hist).alias("first_known_city"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("email_length").over(w_hist).alias("running_email_length"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "email",
        "city",
        "seq_no",
        F.ntile(4).over(w_part).alias("seq_no_bucket"),
        F.lag("seq_no", 1).over(w5).alias("prev_seq_no"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") >= "2025-09-01"), "left_semi")
)

not_flagged_customers = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= "2023-03-27"), "left_anti")
)

enriched = (
    not_flagged_customers
    .select(
        "id",
        "city",
        "seq_no_bucket",
        "latest_rank",
        F.lead("seq_no_bucket").over(w7).alias("next_seq_no_bucket"),
        F.lag("prev_seq_no").over(w7).alias("prev_prev_seq_no"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

stats = (
    enriched
    .groupBy("city")
    .agg(
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.sum(F.when(F.col("city") == "New York", F.col("prev_prev_seq_no"))).alias("new_york_total_prev_prev_seq_no"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("event_count"),
    )
)

result = (
    stats
    .select(
        "city",
        "berlin_events",
        "new_york_total_prev_prev_seq_no",
        "city_list",
        "event_count",
        F.when(F.col("city").rlike("^(Berlin|Singapore)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("new_york_total_prev_prev_seq_no").desc())
)
