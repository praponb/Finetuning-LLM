from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last())

with_windows = (
    df_store_sales
    .select(
        "*",
        F.last("revenue", ignorenulls=True).over(w).alias("last_known_revenue"),
        F.dense_rank().over(w2).alias("revenue_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.bit_and("units").alias("units_bit_and"),
    )
)
