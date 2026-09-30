from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_last())

deduped = (
    df_store_sales
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("revenue") >= 3200, "HIGH").otherwise("LOW").alias("revenue_tier"),
        "product_id",
    )
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .filter(F.col("revenue_p95") > 0)
    .orderBy("revenue_p95")
)
