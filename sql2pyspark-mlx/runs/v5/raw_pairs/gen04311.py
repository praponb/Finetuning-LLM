from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("last3_sum_cost", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-09-23").cast("date")) & F.col("cost").isNotNull())
    .select(
        "*",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
    )
)

enriched = (
    filtered
    .withColumn("last3_max_cost", F.max("cost").over(w))
    .withColumn("last3_sum_cost", F.sum("cost").over(w))
    .withColumn("last3_avg_cost", F.avg("cost").over(w))
    .withColumn("cost_bucket", F.ntile(3).over(w2))
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_id") == 31)
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.cost",
        "src.last3_max_cost",
        "src.last3_sum_cost",
        "src.last3_avg_cost",
        "src.cost_bucket",
        F.col("p.discount"),
    )
)

enriched_shipments_2 = (
    enriched_shipments
    .select(
        "*",
        F.sum("last3_avg_cost").over(w3).alias("last3_sum_last3_avg_cost"),
        F.max("last3_avg_cost").over(w3).alias("last3_max_last3_avg_cost"),
        F.avg("last3_avg_cost").over(w3).alias("last3_avg_last3_avg_cost"),
    )
)

result = (
    enriched_shipments_2
    .groupBy(
        F.when(F.col("last3_avg_last3_avg_cost") >= 88, "PREMIUM").otherwise("STANDARD").alias("last3_avg_last3_avg_cost_tier"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.size(F.collect_set("carrier")).alias("num_carriers_2"),
        F.round(F.sum("last3_sum_last3_avg_cost") / F.nullif(F.count(F.when(F.col("carrier") == "FedEx", 1)), F.lit(0)), 2).alias("last3_sum_last3_avg_cost_per_fedex"),
        F.percentile_approx("last3_max_last3_avg_cost", [0.25, 0.5, 0.75]).alias("last3_max_last3_avg_cost_quartiles"),
    )
    .orderBy(F.col("last3_avg_last3_avg_cost_tier").desc())
)
