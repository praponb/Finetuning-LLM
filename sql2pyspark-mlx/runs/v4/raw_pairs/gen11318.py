from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 4) | (F.col("units") >= 37))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
    .drop("cnt")
    .orderBy(F.col("total_units").desc())
)
