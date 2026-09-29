from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("units_tier").orderBy(F.col("num_rows").desc())

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 38, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.when(F.col("revenue") >= 4300, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
    )
    .filter(F.col("corr_units_revenue") >= 0)
    .withColumn("num_rows_dense_rank", F.dense_rank().over(w))
)
