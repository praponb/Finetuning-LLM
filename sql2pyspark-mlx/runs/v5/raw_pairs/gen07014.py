from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"), "category")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
    )
    .filter(F.col("initech_price") > 23)
    .select("launch_month", "category", "acme_cost", "initech_price")
    .orderBy("initech_price", "launch_month", "category")
    .limit(20)
)
