from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.lower("product_name").like("%ack%"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("cost").cast("int").alias("cost_int"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("cost_int") >= 64, "LARGE").otherwise("SMALL").alias("cost_int_tier"),
        "brand",
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
        F.countDistinct("category").alias("distinct_categories"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
        F.sum("cost").alias("total_cost"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
    .filter(F.col("sports_count") < 1000)
    .orderBy(F.col("electronics_count").desc_nulls_first())
)
