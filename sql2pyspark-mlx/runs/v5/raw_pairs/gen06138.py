from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"), "store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.avg("units").alias("avg_units"),
    )
)
