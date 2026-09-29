from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "*",
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        F.col("c.launch_date"),
        F.col("c.cost").alias("product_cost"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", F.col("c.brand").alias("product_brand"))
)

with_latest_suppliers = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.product_brand",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

summary = (
    with_latest_suppliers
    .groupBy(
        F.when(F.col("product_stock_qty") >= 307, "PREMIUM").otherwise("STANDARD").alias("product_stock_qty_tier"),
        "product_brand",
    )
    .agg(
        F.sum(F.when(F.col("product_brand") == "Initech", F.col("product_stock_qty"))).alias("initech_total_product_stock_qty"),
        F.count_if(F.col("product_brand") == "Stark").alias("stark_events"),
    )
)

result = (
    summary
    .select(
        "product_stock_qty_tier",
        "product_brand",
        "initech_total_product_stock_qty",
    )
    .orderBy("product_stock_qty_tier")
)
