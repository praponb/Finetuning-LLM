from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("category") == "Home")
    .withColumn("brand_group", F.when(F.col("brand").isin(["Stark", "Globex"]), "Tier 1").otherwise("Other"))
)

summary = (
    base
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.min("cost").alias("min_cost"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = summary.filter(F.col("min_cost") > 460)
