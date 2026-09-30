from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("product_id").orderBy("least_units_revenue", "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w4 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("units") >= 20)
    .withColumn("least_units_revenue", F.least("units", "revenue"))
)

result = (
    filtered
    .select(
        "*",
        F.min("least_units_revenue").over(w).alias("product_id_min_least_units_revenue"),
        F.row_number().over(w2).alias("row_seq"),
        F.lag("revenue", 1).over(w3).alias("prev_revenue"),
        F.first("least_units_revenue", ignorenulls=True).over(w4).alias("first_known_least_units_revenue"),
    )
)
