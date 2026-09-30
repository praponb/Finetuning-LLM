from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "London"]))
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", ""))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

base_suppliers = (
    base
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "supplier_name_lower",
        "supplier_name_slug",
        F.round((F.col("rating") * 1.07), 2).alias("rating_adjusted"),
    )
)

latest_match = (
    base_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.supplier_name_lower",
        "s.rating_adjusted",
        F.col("c.stock_qty"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.rating_adjusted",
        "s.stock_qty",
        "s.product_launch_date",
        F.col("c.launch_date").alias("product_launch_date_2"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "country",
        "rating_adjusted",
        "stock_qty",
        "product_launch_date",
        "product_launch_date_2",
    )
    .orderBy(F.col("rating_adjusted").desc_nulls_first())
)
