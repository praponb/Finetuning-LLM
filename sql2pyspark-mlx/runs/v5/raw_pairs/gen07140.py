from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-10").cast("date"))
    .select(
        "*",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.last("revenue", ignorenulls=True).over(w2).alias("last_known_revenue"),
    )
)
