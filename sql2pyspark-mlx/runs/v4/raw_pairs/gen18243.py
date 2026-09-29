from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("price") > 690)
    .withColumn("product_name_upper", F.upper("product_name"))
)

category_summary = (
    filtered_products
    .groupBy("category", "supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
)

result = (
    category_summary
    .filter(F.col("sports_count") < 9)
    .select("category", "supplier_id", "brand_list", "sports_count")
    .orderBy("sports_count")
)
