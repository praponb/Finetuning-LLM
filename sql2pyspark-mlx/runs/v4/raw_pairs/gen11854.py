from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("units", "id")
w4 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last(), "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-25").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
        F.when(F.col("units") >= 36, "High").when(F.col("units") >= 14, "Medium").otherwise("Low").alias("units_tier"),
    )
)

scored = (
    valid
    .withColumn("cumulative_revenue_per_unit", F.sum("revenue_per_unit").over(w))
    .withColumn("running_units", F.sum("units").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("cumulative_revenue_per_unit_2", F.sum("revenue_per_unit").over(w4))
)

valid_store_sales = (
    scored
    .filter(F.col("sale_date") >= F.lit("2024-02-05").cast("date"))
    .withColumn("revenue_per_revenue_per_unit", (F.col("revenue") / F.nullif("revenue_per_unit", F.lit(0))))
)

latest = (
    valid_store_sales
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .withColumn("cumulative_revenue_per_unit_tier", F.when(F.col("cumulative_revenue_per_unit") >= 306, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date"))
    .groupingSets(
        [["cumulative_revenue_per_unit_tier", "activity_month"], ["cumulative_revenue_per_unit_tier"]],
        "cumulative_revenue_per_unit_tier",
        "activity_month",
    )
    .agg(
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.avg("cumulative_revenue_per_unit").alias("avg_cumulative_revenue_per_unit"),
        F.percentile_approx("running_units", 0.95).alias("running_units_p95"),
    )
)
