from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("city", "product_brand").orderBy(F.col("rating").desc(), "id")
w4 = Window.orderBy(F.col("last3_avg_rating").desc(), "id")
w5 = Window.partitionBy("product_brand", "category").orderBy("last3_sum_rating", "id")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").isNull())
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", ""))
    .withColumn("country_supplier_name_key", F.concat_ws("-", "country", "supplier_name"))
)

with_latest = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.brand").alias("product_brand"))
)

with_windows = (
    with_latest
    .filter(F.col("product_brand") == "Initech")
    .withColumn("last3_count_rating", F.count("rating").over(w2))
    .withColumn("last3_avg_rating", F.avg("rating").over(w2))
    .withColumn("last3_sum_rating", F.sum("rating").over(w2))
    .withColumn("last3_min_rating", F.min("rating").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

enriched = (
    with_windows
    .select(
        "id",
        "supplier_name_slug",
        "country_supplier_name_key",
        "category",
        "last3_count_rating",
        "last3_avg_rating",
        "last3_min_rating",
        F.row_number().over(w4).alias("event_seq_2"),
        F.lag("last3_avg_rating", 1).over(w5).alias("prev_last3_avg_rating"),
        F.round((F.col("last3_avg_rating") / F.sum("last3_avg_rating").over(w6)), 4).alias("last3_avg_rating_share"),
    )
)

with_latest_suppliers = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
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
        F.when(F.col("product_cost") >= 670, "PREMIUM").otherwise("STANDARD").alias("product_cost_tier"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.round(F.sum("last3_count_rating") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("last3_count_rating_per_grocery"),
    )
    .filter(F.col("fashion_events") >= 1)
    .orderBy(F.col("last3_count_rating_per_grocery").desc())
)
