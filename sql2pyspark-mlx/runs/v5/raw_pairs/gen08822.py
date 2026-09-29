from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w4 = Window.partitionBy("revenue_tier").orderBy(F.col("revenue_per_unit_p95").desc())

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-07-31").cast("date")) & F.col("store_id").isin([1, 2, 5, 6]))
    .withColumn("revenue_per_unit", (F.col("units") + F.col("revenue")))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    deduped
    .filter((F.col("sale_date") >= F.lit("2023-09-15").cast("date")) & (F.col("revenue") < 1600))
    .withColumn("revenue_int", F.col("revenue").cast("int"))
    .withColumn("revenue_per_unit_2", F.round((F.col("revenue") / F.col("units")), 2))
)

enriched = (
    valid
    .filter(F.col("revenue_int") > 30)
    .select(
        "*",
        F.sum("revenue_int").over(w2).alias("product_id_sum_revenue_int"),
        F.lag("revenue_per_unit_2", 1).over(w3).alias("prev_revenue_per_unit_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("revenue") >= 3600, "HIGH").otherwise("LOW").alias("revenue_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("revenue_per_unit", 0.95).alias("revenue_per_unit_p95"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.percentile_approx("revenue_per_unit_2", [0.25, 0.5, 0.75]).alias("revenue_per_unit_2_quartiles"),
    )
    .filter(F.col("corr_revenue_units") > 0)
    .withColumn("revenue_per_unit_p95_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("revenue_per_unit_p95").desc())
)
