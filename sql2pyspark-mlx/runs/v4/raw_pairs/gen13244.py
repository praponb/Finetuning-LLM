from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status")
w2 = Window.orderBy(F.col("cost").desc())
w3 = Window.partitionBy("carrier", "order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "carrier").orderBy(F.col("running_cost").asc_nulls_last(), "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-04-20"))) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.concat("carrier", F.lit(" "), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

enriched = (
    filtered
    .withColumn("status_count_cost", F.count("cost").over(w_ordered))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("running_cost", F.sum("cost").over(w3))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

summary = (
    ranked
    .groupBy(
        F.when(F.col("status_count_cost") >= 17, "PREMIUM").otherwise("STANDARD").alias("status_count_cost_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("status_count_cost", 0.5).alias("status_count_cost_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("status_count_cost").alias("max_status_count_cost"),
        F.percentile_approx("status_count_cost", [0.25, 0.5, 0.75]).alias("status_count_cost_quartiles"),
    )
)

result = (
    summary
    .select(
        "status_count_cost_tier",
        "status",
        "status_count_cost_p50",
        "distinct_ids",
        "max_status_count_cost",
    )
    .orderBy("max_status_count_cost", F.col("status_count_cost_tier").desc())
)
