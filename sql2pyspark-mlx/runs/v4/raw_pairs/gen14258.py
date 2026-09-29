from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_first())
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("corr_units_revenue", "id")
w7 = Window.orderBy("last_known_units", "id")
w8 = Window.partitionBy("product_id").orderBy("first_known_units_avg_5", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy("revenue_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-10-27").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 3900, "High").when(F.col("revenue") >= 1900, "Medium").otherwise("Low").alias("revenue_band"),
        F.date_sub("sale_date", 4).alias("sale_date_minus_4d"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.rank().over(w).alias("revenue_rank"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.last("units", ignorenulls=True).over(w3).alias("last_known_units"),
        F.avg("units").over(w4).alias("units_avg_5"),
    )
)

metrics = (
    enriched
    .withColumn("first_known_units_avg_5", F.first("units_avg_5", ignorenulls=True).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("event_seq_2", F.row_number().over(w7))
    .withColumn("product_id_avg_corr_units_revenue", F.avg("corr_units_revenue").over(w2))
    .withColumn("corr_units_revenue_2", F.corr("units", "revenue").over(w2))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.stock_qty"), F.col("p.cost"), F.col("p.category"))
)

result = (
    with_dims
    .withColumn("corr_units_revenue_2_avg_5", F.avg("corr_units_revenue_2").over(w8))
    .withColumn("last_known_event_seq", F.last("event_seq", ignorenulls=True).over(w9))
)
