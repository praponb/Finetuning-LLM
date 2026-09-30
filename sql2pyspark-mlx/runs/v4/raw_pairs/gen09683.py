from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("sale_date", "id")
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand", "product_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("prev_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("units_rounded").desc_nulls_first())
w7 = Window.partitionBy("brand").orderBy(F.col("revenue_per_unit").desc())
w8 = Window.partitionBy("product_id").orderBy("price", "id")
w9 = Window.partitionBy("store_id", "brand").orderBy("units_rounded", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 6]))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
    .withColumn("units_rounded", F.round("units", 2))
)

enriched = (
    base
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.brand"), F.col("d.price"))
)

with_windows = (
    enriched
    .withColumn("prev_units", F.lag("units", 1).over(w))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w2))
    .withColumn("first_known_units_rounded", F.first("units_rounded", ignorenulls=True).over(w3))
)

enriched_store_sales = (
    with_windows
    .withColumn("running_prev_units", F.sum("prev_units").over(w4))
    .withColumn("cumulative_units", F.sum("units").over(w5))
    .withColumn("units_rounded_dense_rank", F.dense_rank().over(w6))
    .withColumn("revenue_per_unit_percent_rank", F.percent_rank().over(w7))
)

result = (
    enriched_store_sales
    .select(
        "*",
        F.lag("revenue_per_unit_percent_rank", 2).over(w8).alias("prev_revenue_per_unit_percent_rank"),
        F.avg("first_known_price").over(w9).alias("first_known_price_avg_3"),
    )
)
