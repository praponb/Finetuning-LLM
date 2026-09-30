from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Umbrella", "Initech"]))
    .groupBy(
        "category",
        F.when(F.col("category").isin(["Books", "Fashion"]), "core").otherwise("other").alias("category_group"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") > 52)
    .orderBy("category_group")
)
