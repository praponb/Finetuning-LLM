from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy("seq_no", "id").rowsBetween(-3, Window.currentRow)
w_recent = Window.partitionBy("browser").orderBy("seq_no", "id")
w6 = Window.partitionBy("browser").orderBy("started_at", "id")
w7 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

enriched = (
    df_sessions
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
)

eligible = (
    enriched
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page").isin(["/home", "/search", "/checkout"])), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.avg("seq_no").over(w4).alias("seq_no_avg_3"),
        F.ntile(3).over(w_recent).alias("seq_no_bucket"),
    )
)

gaps = (
    metrics
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w6).isNull() | (F.col("started_at") > F.lag("started_at").over(w6) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w7))

filtered = (
    islands
    .filter((F.col("started_at") >= F.lit("2023-06-28 05:00:00").cast("timestamp")) & (F.col("seq_no_avg_3") != 6))
    .select(
        "*",
        F.when(F.col("seq_no") >= 7, "High").when(F.col("seq_no") >= 4, "Medium").otherwise("Low").alias("seq_no_band"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_filled"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.age"), F.col("p.segment"))
)

filtered_sessions = (
    joined
    .filter((F.col("started_at") > F.lit("2023-09-03").cast("date")) & (F.col("session_no") == 5))
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "first_known_country",
        "seq_no",
        "seq_no_2",
        "seq_no_bucket",
        "new_group_flag",
        "session_no",
        "seq_no_band",
        "age",
        "segment",
        F.round((F.col("seq_no_avg_3") * 1.07), 2).alias("seq_no_avg_3_adjusted"),
        F.substring("device", 1, 4).alias("device_prefix"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

result = (
    filtered_sessions
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "first_known_country",
        "seq_no",
        "seq_no_2",
        "seq_no_bucket",
        "session_no",
        F.when(F.col("country").rlike("^(US|SG)$"), "EXTENDED").when(F.col("country").rlike("^(TH|JP)$"), "GROUP_A").otherwise("PRIORITY").alias("country_zone"),
        F.col("min_account_balance"),
    )
    .orderBy(F.col("device").desc())
)
