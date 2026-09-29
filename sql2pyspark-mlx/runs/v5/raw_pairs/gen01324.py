from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("product_name_slug", F.regexp_replace("product_name", " ", "-"))
)

result = (
    cleaned
    .rollup("supplier_id")
    .agg(
        F.max(F.when(F.col("brand") == "Acme", F.col("cost"))).alias("acme_max_cost"),
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
    )
    .orderBy(F.col("acme_max_cost").desc_nulls_last())
)
