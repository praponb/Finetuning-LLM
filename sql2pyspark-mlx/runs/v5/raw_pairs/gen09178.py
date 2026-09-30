from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("segment", "country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("segment")
w7 = Window.partitionBy("segment").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.count("price").over(w2).alias("price_count_1h"),
        F.max("product_id").over(w3).alias("product_id_max_30m"),
    )
)

base = (
    with_windows
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-02"))) & F.col("customer_id").isNotNull())
    .select("*", F.date_trunc("YEAR", "created_at").alias("created_at_period"))
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.segment") == "Corporate")
    .select(
        "s.id",
        "s.customer_id",
        "s.created_at",
        "s.coupon_codes",
        "s.product_id",
        "s.qty",
        "s.price_count_1h",
        F.col("d.segment"),
        F.col("d.country"),
        F.col("d.signup_date"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "price_count_1h",
        F.count("qty").over(w4).alias("qty_count_30m"),
        F.stddev_samp("product_id").over(w5).alias("country_stddev_samp_product_id"),
        F.round((F.col("product_id") / F.sum("product_id").over(w6)), 4).alias("product_id_share"),
        F.count("product_id").over(w7).alias("product_id_count_15m"),
        F.round((F.col("qty") / F.sum("qty").over(w5)), 4).alias("qty_share"),
    )
)

result = (
    metrics
    .select("customer_id", "product_id_share", "qty_share")
    .unpivot(
        ids=["customer_id"],
        values=["product_id_share", "qty_share"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
