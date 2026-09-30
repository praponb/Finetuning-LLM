from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

ranked = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "supplier_name")
)

filtered = (
    ranked
    .filter(~F.col("supplier_name").like("%Suppl%"))
    .select("*", F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.launch_date"))
)

with_latest_suppliers = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.cost").alias("product_cost"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

result = (
    with_latest_suppliers
    .groupBy(
        F.when(F.col("product_cost") >= 550, "HIGH").otherwise("LOW").alias("product_cost_tier"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.percentile_approx("product_cost", 0.95).alias("product_cost_p95"),
        F.min("product_cost").alias("min_product_cost"),
        F.countDistinct("category").alias("distinct_categories"),
    )
    .orderBy(F.col("product_cost_p95").desc())
)
