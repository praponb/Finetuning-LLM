from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("num_cities").desc())

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").like("%Lapto%")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("product_cost"))
)

matched_keys = (
    latest_match
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("country") == "SG")
        .select("id", "city")
    )
)

verified = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (~F.col("c.product_name").like("%Sho%")), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        "country",
        F.when(F.col("product_cost") >= 740, "PREMIUM").otherwise("STANDARD").alias("product_cost_tier"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.round(F.avg("product_cost"), 2).alias("avg_product_cost"),
        F.max(F.when(F.col("country") == "JP", F.col("product_cost"))).alias("jp_max_product_cost"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .withColumn("num_cities_percent_rank", F.percent_rank().over(w2))
    .orderBy("avg_product_cost")
)
