from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .groupBy(
        F.when(F.col("units") >= 34, "high").when(F.col("units") >= 10, "mid").otherwise("low").alias("units_bucket"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.avg("units").alias("avg_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("last_sale_date", "sale_month", "units_bucket")
    .limit(1)
)
