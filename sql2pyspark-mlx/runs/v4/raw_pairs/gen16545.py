from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.cost") < 41), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.method") == "card"), "left_semi")
)

enriched = (
    eligible
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.name"), F.col("c.segment"))
)

base = (
    enriched
    .filter((F.col("order_date") >= F.lit("2023-12-02").cast("date")) & F.col("channel").isNotNull())
    .withColumn("order_date_dayofweek", F.dayofweek("order_date"))
)

result = (
    base
    .groupBy(
        F.when(F.col("discount") >= 0.36, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.count("*").alias("event_count"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("amount_per_mobile"),
    )
    .orderBy("amount_per_mobile")
)
