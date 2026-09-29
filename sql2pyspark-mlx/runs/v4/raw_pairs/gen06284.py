from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_shipments
    .withColumn("running_cost", F.sum("cost").over(w))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w3))
)

valid = (
    enriched
    .filter(F.col("cost") < 40)
    .select(
        "*",
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

base = (
    valid
    .filter(F.col("cost").between(73, 76))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "running_cost",
        "cost_cume_dist",
        "first_known_carrier",
        "carrier_upper",
        "carrier_clean",
        F.lower("carrier").alias("carrier_lower"),
    )
)

result = (
    base
    .withColumn("cost_tier", F.when(F.col("cost") >= 93, "LARGE").otherwise("SMALL"))
    .withColumn("running_cost_tier", F.when(F.col("running_cost") >= 427, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["cost_tier", "running_cost_tier", "carrier"], ["cost_tier", "running_cost_tier"], ["cost_tier"], []],
        "cost_tier",
        "running_cost_tier",
        "carrier",
    )
    .agg(
        F.percentile_approx("cost_cume_dist", 0.9).alias("cost_cume_dist_p90"),
        F.percentile_approx("cost_cume_dist", 0.9).alias("cost_cume_dist_p90_2"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count("*").alias("row_count"),
        F.grouping("cost_tier").alias("gid"),
    )
    .filter(F.col("cost_cume_dist_p90_2") < 0)
    .orderBy(F.col("running_cost_tier").desc_nulls_first())
)
