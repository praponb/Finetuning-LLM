from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") >= 2024)
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
        F.when(F.col("cost") >= 760, "high").when(F.col("cost") >= 250, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("price")).otherwise(0)).alias("stark_price"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("stark_price") < 60)
)
