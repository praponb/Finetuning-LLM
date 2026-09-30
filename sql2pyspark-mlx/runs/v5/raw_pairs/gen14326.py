from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_per_unit").asc_nulls_last())
w4 = Window.orderBy("units", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-07-10")))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

result = (
    valid
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("cumulative_revenue_per_unit", F.sum("revenue_per_unit").over(w2))
    .withColumn("revenue_per_unit_percent_rank", F.percent_rank().over(w3))
    .withColumn("prev_units", F.lag("units", 1).over(w4))
)
