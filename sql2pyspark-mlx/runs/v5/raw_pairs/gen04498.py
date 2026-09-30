from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

lines_carts = lines.select("id", "qty", "price", F.explode_outer("coupon_codes").alias("tag"))

remaining_keys = (
    lines_carts
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "BUNDLE"))
        .select("id")
    )
)

verified = (
    lines_carts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select("*", F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("price") >= 190, "HIGH").otherwise("LOW").alias("price_tier"),
        F.when(F.col("qty") >= 5, "LARGE").otherwise("SMALL").alias("qty_tier"),
    )
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("qty_tier").asc_nulls_last())
)
