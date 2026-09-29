from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_first(), "id")
w3 = Window.partitionBy("supplier_id", "brand").orderBy("stock_qty", "id")

valid = (
    df_products
    .filter(F.col("cost") <= 590)
    .select(
        "*",
        F.initcap(F.trim("category")).alias("category_title"),
        F.array_join("tags", ", ").alias("tags_text"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "tags_text",
        "brand_clean",
    )
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["country", "brand"], ["country"]],
        F.col("s.country").alias("country"),
        F.col("a.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.cost")).alias("avg_cost"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
