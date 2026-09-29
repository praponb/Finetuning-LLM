from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category").isin(["Fashion", "Electronics"])), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") > "2024-05-01"), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") >= "2025-07-27"), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.city",
        F.col("c.brand").alias("product_brand"),
    )
)

cleaned = (
    with_latest
    .filter(F.col("city").isNotNull() & F.col("country").isin(["US", "DE"]))
    .withColumn("city_filled", F.coalesce("city", F.lit("Unknown")))
    .withColumn("product_brand_country", F.concat("product_brand", F.lit(" - "), "country"))
)

kept_suppliers = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") >= 192), "left_anti")
)

valid = (
    kept_suppliers
    .filter(F.col("product_brand").isNotNull() & (F.col("country") != "UK"))
    .select(
        "*",
        F.concat_ws(",", "country", "supplier_name").alias("country_supplier_name_key"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "product_brand",
        "product_brand_country",
        "country_supplier_name_key",
        "city_clean",
    )
    .orderBy(F.col("city").desc_nulls_first())
)
