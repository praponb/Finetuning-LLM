from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w4 = Window.orderBy(F.col("revenue_bucket").desc_nulls_last())
w5 = Window.partitionBy("store_id").orderBy("next_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("next_revenue").asc_nulls_first())
w8 = Window.partitionBy("store_id")
w9 = Window.partitionBy("product_id").orderBy(F.col("units_avg_2").desc_nulls_last(), "id")

enriched = (
    df_store_sales
    .select(
        "*",
        F.lead("revenue", 1).over(w).alias("next_revenue"),
        F.avg("units").over(w2).alias("units_avg_2"),
        F.ntile(10).over(w3).alias("revenue_bucket"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.sqft"), F.col("d.city"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.dense_rank().over(w4).alias("revenue_bucket_dense_rank"),
        F.sum("units_avg_2").over(w5).alias("running_units_avg_2"),
        F.sum("revenue").over(w6).alias("cumulative_revenue"),
        F.percent_rank().over(w7).alias("next_revenue_percent_rank"),
        F.round((F.col("units") / F.sum("units").over(w8)), 4).alias("units_share"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

summary = (
    deduped
    .groupBy(
        F.when(F.col("revenue_bucket_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("revenue_bucket_dense_rank_tier"),
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.sum("units").alias("total_units"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("units_bit_and") >= 1)
)

result = summary.select("revenue_bucket_dense_rank_tier", "units_bit_and")
