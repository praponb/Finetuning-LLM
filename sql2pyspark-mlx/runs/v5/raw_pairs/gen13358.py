from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(((F.lower(F.trim("product_name")) == "water bottle") & F.month("launch_date").isin([1, 3, 8])) | F.upper("product_name").like("%UNN%"))
)

agg = (
    valid_products
    .groupBy("brand")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("cost")).otherwise(0)).alias("fashion_cost"),
    )
)

result = (
    agg
    .filter(F.col("fashion_cost") > 77)
    .orderBy("fashion_cost")
)
