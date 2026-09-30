from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id").orderBy("units_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("units_adjusted").desc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("least_units_revenue", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("units_adjusted_percent_rank").asc_nulls_last())
w7 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy(F.col("running_least_units_revenue").asc_nulls_last())
w9 = Window.partitionBy("city", "store_id").orderBy("units_adjusted_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city").orderBy(F.col("store_id_count_least_units_revenue").desc_nulls_last())
w11 = Window.partitionBy("store_id").orderBy("store_id_count_least_units_revenue", "id")
w_ordered = Window.partitionBy("city").orderBy(F.col("store_id_count_least_units_revenue").desc())
w13 = Window.partitionBy("store_id")

cleaned = (
    df_store_sales
    .filter(F.col("units").between(26, 35))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.least("units", "revenue").alias("least_units_revenue"),
        F.round((F.col("units") * 100), 2).alias("units_adjusted"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count("least_units_revenue").over(w).alias("store_id_count_least_units_revenue"),
        F.sum("least_units_revenue").over(w2).alias("running_least_units_revenue"),
        F.percent_rank().over(w3).alias("units_adjusted_percent_rank"),
    )
)

scored = (
    with_windows
    .withColumn("last_known_store_id_count_least_units_revenue", F.last("store_id_count_least_units_revenue", ignorenulls=True).over(w4))
    .withColumn("units_avg_6", F.avg("units").over(w5))
    .withColumn("units_adjusted_percent_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("units_adjusted_avg_2", F.avg("units_adjusted").over(w7))
    .withColumn("running_least_units_revenue_cume_dist", F.cume_dist().over(w8))
)

enriched = (
    scored
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.city"), F.col("b.store_name"), F.col("b.sqft"))
)

enriched_store_sales = (
    enriched
    .withColumn("running_units_avg_6", F.sum("units_avg_6").over(w9))
    .withColumn("store_id_count_least_units_revenue_dense_rank", F.dense_rank().over(w10))
    .withColumn("store_id_count_least_units_revenue_bucket", F.ntile(4).over(w11))
)

with_dims = (
    enriched_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.category"))
)

result = (
    with_dims
    .withColumn("store_id_count_least_units_revenue_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("store_id_sum_least_units_revenue", F.sum("least_units_revenue").over(w13))
)
