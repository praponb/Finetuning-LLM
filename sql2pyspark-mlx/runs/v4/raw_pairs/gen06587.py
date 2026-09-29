from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("cost").between(34, 35))
    .withColumn("shipped_date_plus_months", F.add_months("shipped_date", 6))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("days_shipped_date_to_delivered_date") >= 79, "PREMIUM").otherwise("STANDARD").alias("days_shipped_date_to_delivered_date_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
