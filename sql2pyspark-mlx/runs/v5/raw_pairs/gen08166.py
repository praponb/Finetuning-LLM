from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") > 1130) & ((F.col("category") != "Books") | (F.col("cost") > 630)))
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
        F.when(F.col("category").isin(["Books", "Electronics"]), "core").otherwise("other").alias("category_group"),
    )
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy("avg_price", "launch_month", "category_group")
    .limit(5)
)
