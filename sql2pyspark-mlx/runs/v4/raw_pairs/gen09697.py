from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(((F.col("revenue") > 1100) & F.col("store_id").isin([4, 5])) | F.col("product_id").isin([1, 5, 7, 10]))
    .withColumn("units_level", F.when(F.col("units") >= 40, "High").when(F.col("units") >= 15, "Medium").otherwise("Low"))
)

store_id_summary = (
    base
    .groupBy(
        "store_id",
        F.when(F.col("units") >= 31, "high").when(F.col("units") >= 12, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
)

result = (
    store_id_summary
    .filter(F.col("row_count") >= 4)
    .orderBy("store_id", F.col("row_count").desc(), "units_bucket")
    .limit(20)
)
