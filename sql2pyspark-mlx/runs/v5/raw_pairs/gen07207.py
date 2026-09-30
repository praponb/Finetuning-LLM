from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("days_since_sale_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc())
w3 = Window.orderBy("units", "id")
w_grp = Window.partitionBy("product_id", "store_id")
w_ordered = Window.partitionBy("store_id").orderBy("product_id_min_units", "id")
w6 = Window.partitionBy("region")
w7 = Window.partitionBy("store_id").orderBy("units_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("region", "store_id").orderBy("revenue_percent_rank", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy("sqft", "id")

base = (
    df_store_sales
    .filter(F.col("revenue").between(2300, 3100))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)

with_windows = (
    base
    .select(
        "*",
        F.avg("units").over(w_running).alias("units_avg_3"),
        F.percent_rank().over(w2).alias("revenue_percent_rank"),
        F.ntile(3).over(w3).alias("units_bucket"),
        F.min("units").over(w_grp).alias("product_id_min_units"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.revenue_percent_rank",
        "e.units_bucket",
        "e.product_id_min_units",
        F.col("s.city"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)

enriched_store_sales = (
    enriched
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.sqft").alias("store_sqft"))
)

metrics = (
    enriched_store_sales
    .select(
        "*",
        F.lead("product_id_min_units", 1).over(w_ordered).alias("next_product_id_min_units"),
        F.min("revenue_percent_rank").over(w6).alias("region_min_revenue_percent_rank"),
        F.sum("product_id_min_units").over(w7).alias("cumulative_product_id_min_units"),
    )
)

with_windows_store_sales = (
    metrics
    .select(
        "id",
        "store_id",
        "revenue_percent_rank",
        "units_bucket",
        "city",
        "region",
        "sqft",
        "store_sqft",
        "next_product_id_min_units",
        F.avg("next_product_id_min_units").over(w8).alias("last7_avg_next_product_id_min_units"),
        F.sum("next_product_id_min_units").over(w8).alias("last7_sum_next_product_id_min_units"),
        F.max("next_product_id_min_units").over(w8).alias("last7_max_next_product_id_min_units"),
        F.min("next_product_id_min_units").over(w8).alias("last7_min_next_product_id_min_units"),
        F.ntile(5).over(w9).alias("sqft_bucket"),
    )
)

wide = (
    with_windows_store_sales
    .groupBy("store_id", "city")
    .pivot("region", ["Central", "East", "South"])
    .agg(
        F.max("last7_max_next_product_id_min_units").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "store_id",
        "city",
        F.col("Central_value_sum").alias("central_value_sum"),
        F.col("Central_cnt").alias("central_cnt"),
        F.col("East_value_sum").alias("east_value_sum"),
        F.col("East_cnt").alias("east_cnt"),
        F.col("South_value_sum").alias("south_value_sum"),
        F.col("South_cnt").alias("south_cnt"),
    )
)

result = (
    wide
    .select("store_id", "city", "central_cnt", "east_value_sum", "south_cnt")
    .orderBy(F.col("store_id").asc_nulls_last())
)
