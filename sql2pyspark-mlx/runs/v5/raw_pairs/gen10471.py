from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(450, 810))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
    )
    .orderBy("globex_count")
)
