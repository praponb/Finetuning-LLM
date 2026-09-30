from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.count("*").alias("cnt"),
    )
    .orderBy("cnt")
)
