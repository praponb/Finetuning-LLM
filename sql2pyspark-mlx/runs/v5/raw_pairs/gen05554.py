from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("category_title", F.initcap(F.trim("category")))
    .withColumn("product_name_upper", F.upper("product_name"))
)

result = (
    filtered
    .alias("a")
    .join(df_suppliers.alias("dim"), F.col("a.supplier_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "category"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("a.category").alias("category"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.cost")).alias("total_cost"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
