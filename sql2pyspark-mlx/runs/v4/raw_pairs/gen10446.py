from pyspark.sql import functions as F

metrics_long = (
    df_products
    .select("id", "category", "cost", F.col("price").alias("PRICE"))
    .unpivot(["id", "category"], ["cost", "PRICE"], "measure", "value")
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .groupBy("category")
    .agg(
        F.avg("value").alias("avg_value"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
)
