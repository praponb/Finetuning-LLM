from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy(F.col("product_stock_qty").asc_nulls_last(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.lower("city").alias("city_lower"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

with_last_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

cleaned = (
    with_last_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_prefix", F.substring("country", 1, 2))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    deduped
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("country") == "SG")
        .select("country", "id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "supplier_name",
        "country",
        "city",
        "supplier_name_title",
        "city_lower",
        "cost",
        "product_stock_qty",
        "rank_in_group",
    )
    .orderBy(F.col("country").desc(), "product_stock_qty")
)
