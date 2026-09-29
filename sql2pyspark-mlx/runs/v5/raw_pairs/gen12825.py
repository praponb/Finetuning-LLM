from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("category", "product_category").orderBy("product_launch_date", "id")
w4 = Window.partitionBy("product_category").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_category")
w6 = Window.partitionBy("product_category_min_seq_no_tier").orderBy(F.col("avg_product_category_min_seq_no").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "rating",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand") == "Umbrella"), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.category").alias("product_category"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "supplier_name_normalized",
        "product_category",
        F.lead("rating").over(w_part).alias("next_rating"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

with_windows_suppliers = (
    with_windows
    .select(
        "id",
        "product_category",
        "seq_no",
        F.sum("seq_no").over(w4).alias("cumulative_seq_no"),
        F.min("seq_no").over(w_recent).alias("product_category_min_seq_no"),
    )
)

result = (
    with_windows_suppliers
    .groupBy(
        F.when(F.col("product_category_min_seq_no") >= 7, "HIGH").otherwise("LOW").alias("product_category_min_seq_no_tier"),
        "seq_no",
        F.when(F.col("seq_no") >= 7, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("product_category") == "Home", 1)).alias("home_count"),
        F.size(F.collect_set("product_category")).alias("num_product_categories"),
        F.round(F.avg("product_category_min_seq_no"), 2).alias("avg_product_category_min_seq_no"),
    )
    .withColumn("avg_product_category_min_seq_no_percent_rank", F.percent_rank().over(w6))
)
