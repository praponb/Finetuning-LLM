from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("supplier_id").isNotNull())

summary = (
    valid_products
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    summary
    .select("category", "category_values", "first_launch_date")
    .orderBy(F.col("first_launch_date").desc(), "category")
    .limit(1)
)
