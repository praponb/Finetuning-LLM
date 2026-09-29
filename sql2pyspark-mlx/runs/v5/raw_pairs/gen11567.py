from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.count("*").alias("cnt"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .orderBy(F.col("cnt").desc(), "launch_year")
    .limit(10)
)
