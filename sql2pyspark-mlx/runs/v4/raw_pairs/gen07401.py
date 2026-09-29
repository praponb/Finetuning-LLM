from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "city").orderBy("days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("days_since_sale_date", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-04")))
    .select(
        "id",
        "store_id",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

enriched = (
    valid
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.city"), F.col("b.region"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("days_since_sale_date").over(w).alias("running_days_since_sale_date"),
        F.avg("days_since_sale_date").over(w2).alias("days_since_sale_date_avg_5"),
    )
)

result = (
    with_windows
    .groupBy("city")
    .pivot("region", ["East", "Central", "North"])
    .agg(F.avg("revenue").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
