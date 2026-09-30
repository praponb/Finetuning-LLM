from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("revenue_tier").orderBy(F.col("row_count").desc())

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3900, "HIGH").otherwise("LOW").alias("revenue_tier"),
        "product_id",
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.bit_or("units").alias("units_bit_or"),
        F.max("revenue").alias("max_revenue"),
    )
    .withColumn("row_count_rank", F.rank().over(w))
    .orderBy(F.col("row_count").desc_nulls_last())
)
