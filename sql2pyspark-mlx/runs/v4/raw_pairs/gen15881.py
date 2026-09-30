from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

matched_keys = (
    with_last
    .select("category", "id")
    .intersectAll(
        df_products
        .filter(F.col("category").isin(["Fashion", "Electronics"]))
        .select("category", "id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.supplier_name"), F.col("p2.rating"))
)

valid = (
    with_dims
    .filter((F.col("launch_date") > F.lit("2024-01-13").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("DIRECT")))
)

result = (
    valid
    .select(
        "brand",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        "supplier_name",
        "rating",
        "rating_filled",
        "brand_filled",
    )
    .orderBy(F.col("launch_date").asc_nulls_last())
)
