from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("category").isin(["Books", "Sports"]), "tier_1").otherwise("other").alias("category_group"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
    )
    .filter(F.col("avg_cost") < 510)
    .select("category_group", "avg_cost", "pct_initech")
    .orderBy(F.col("pct_initech").desc(), "category_group")
    .limit(3)
)
