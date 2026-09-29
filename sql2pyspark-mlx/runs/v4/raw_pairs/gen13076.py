from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").asc_nulls_last())
w3 = Window.partitionBy("product_id", "region").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("activity_month").orderBy(F.col("avg_units").desc())

joined = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.opened_date"), F.col("p.sqft"), F.col("p.region"))
)

with_windows = (
    joined
    .select(
        "*",
        F.avg("revenue").over(w).alias("revenue_avg_2_prior"),
        F.percent_rank().over(w2).alias("revenue_percent_rank"),
        F.sum("units").over(w3).alias("cumulative_units"),
    )
)

joined_store_sales = (
    with_windows
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.store_name"), F.col("b.city"))
)

result = (
    joined_store_sales
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.round(F.avg("revenue_percent_rank"), 2).alias("avg_revenue_percent_rank"),
        F.max(F.when(F.col("city") == "London", F.col("cumulative_units"))).alias("london_max_cumulative_units"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("london_max_cumulative_units") > 0)
    .withColumn("avg_units_rank", F.rank().over(w4))
)
