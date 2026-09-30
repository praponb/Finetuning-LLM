from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") > 3.3))
    .select(
        "*",
        F.when(F.col("country").isin(["DE", "JP"]), "Tier 1").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

filtered = (
    valid
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "country_group",
        "resolved_city",
        F.when(F.col("country").isin(["DE", "UK"]), "Priority").otherwise("Other").alias("country_group_2"),
    )
)

scored = (
    filtered
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("prev_rating", F.lag("rating", 1, 0).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

filtered_suppliers = (
    scored
    .filter(F.col("city").isNotNull() & (F.col("row_seq") > 5))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "country_group",
        "country_group_2",
        "prev_rating",
        "row_seq",
        (F.col("rating_share") + F.col("row_seq")).alias("rating_share_plus_row_seq"),
        F.substring("supplier_name", 1, 2).alias("supplier_name_prefix"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

kept = (
    filtered_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") == 111), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

with_last_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.stock_qty").alias("product_stock_qty"))
)

result = (
    with_last_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "prev_rating",
        "row_seq",
        "supplier_name_prefix",
        "city_clean",
        "cost",
        "product_stock_qty",
    )
)
