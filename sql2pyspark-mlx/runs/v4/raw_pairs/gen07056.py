from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("country").orderBy(F.col("stock_qty").asc_nulls_first())

cleaned = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Tokyo"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price").alias("product_price"), F.col("c.stock_qty"))
)

filtered = (
    latest_match
    .filter(F.col("rating").between(2.8, 3.9))
    .withColumn("stock_qty_times_rating", (F.col("stock_qty") * F.col("rating")))
    .withColumn("city_filled", F.coalesce("city", F.lit("NONE")))
)

base = (
    filtered
    .filter(F.length("supplier_name") >= 12)
    .select(
        "id",
        "country",
        "city",
        "country_upper",
        "resolved_city",
        "product_price",
        "stock_qty",
        "city_filled",
        F.substring("supplier_name", 1, 4).alias("supplier_name_prefix"),
    )
)

kept_keys = (
    base
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") < 3.9)
        .select("country")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("country_sum_stock_qty", F.sum("stock_qty").over(w2))
    .withColumn("city_min_stock_qty", F.min("stock_qty").over(w3))
    .withColumn("city_max_product_price", F.max("product_price").over(w4))
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w5))
    .withColumn("country_count_product_price", F.count("product_price").over(w2))
)

result = (
    scored
    .select(
        "id",
        "city",
        "resolved_city",
        "stock_qty",
        "supplier_name_prefix",
        "country_sum_stock_qty",
        "country_count_product_price",
        F.when(F.col("city").rlike("^(Tokyo|New York)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc(), "stock_qty")
)
