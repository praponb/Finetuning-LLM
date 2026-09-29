from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").asc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_last())
w3 = Window.partitionBy("quarter_start").orderBy(F.col("avg_units").desc())

metrics = (
    df_store_sales
    .withColumn("revenue_cume_dist", F.cume_dist().over(w))
    .withColumn("units_dense_rank", F.dense_rank().over(w2))
)

result = (
    metrics
    .groupBy(
        F.date_trunc("quarter", "sale_date").alias("quarter_start"),
        "store_id",
        F.when(F.col("units") >= 44, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.avg("units").alias("avg_units"),
        F.count("*").alias("cnt"),
        F.round(F.avg("revenue_cume_dist"), 2).alias("avg_revenue_cume_dist"),
    )
    .withColumn("avg_units_dense_rank", F.dense_rank().over(w3))
)
