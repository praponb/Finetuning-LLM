from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("product_price").desc_nulls_first())
w3 = Window.partitionBy("brand").orderBy("rating", "id")
w4 = Window.partitionBy("brand").orderBy("product_price", "id")
w5 = Window.partitionBy("country", "brand")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("city", "country")
w8 = Window.partitionBy("city")

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"), F.col("c.price").alias("product_price"))
)

scored = (
    with_latest
    .select(
        "*",
        F.rank().over(w2).alias("product_price_rank"),
        F.ntile(5).over(w3).alias("rating_bucket"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

kept_keys = (
    scored
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("country").isin(["UK", "DE"]))
        .select("id", "city")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.count("rating").over(w5).alias("country_count_rating"),
        F.stddev_samp("rating").over(w6).alias("country_stddev_samp_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w7)), 4).alias("rating_share"),
        F.round((F.col("product_price") / F.sum("product_price").over(w8)), 4).alias("product_price_share"),
    )
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("rating_share") >= 0.76, "HIGH").otherwise("LOW").alias("rating_share_tier"),
    )
    .agg(
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.round(F.sum("product_price_share") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("product_price_share_per_globex"),
    )
)

result = (
    daily
    .select(
        "rating_share_tier",
        "num_brands",
        "umbrella_rows",
        "product_price_share_per_globex",
    )
    .orderBy(F.col("num_brands").desc_nulls_first(), F.col("umbrella_rows").desc())
)
