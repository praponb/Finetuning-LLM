from pyspark.sql import functions as F

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") == "Grocery"), "left_anti")
)

result = (
    eligible
    .rollup(
        "country",
        "city",
        F.when(F.col("rating") >= 4.0, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.avg(F.when(F.col("city") == "New York", F.col("rating"))).alias("new_york_avg_rating"),
        F.max(F.when(F.col("city") == "Chiang Mai", F.col("rating"))).alias("chiang_mai_max_rating"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .filter(F.col("th_count") > 1)
    .orderBy(F.col("chiang_mai_max_rating").desc_nulls_first())
)
