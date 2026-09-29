from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost")).otherwise(0)).alias("globex_cost"),
    )
    .filter(F.col("globex_cost") > 70)
    .orderBy(F.col("globex_cost").desc(), "launch_month")
    .limit(1)
)
