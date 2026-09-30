from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")

ranked = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"))
)

valid = (
    enriched
    .filter((F.col("last_restock") > F.lit("2023-07-27").cast("date")) & F.col("category").isNotNull())
    .select(
        "*",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.coalesce("category", F.lit("DIRECT")).alias("category_filled"),
    )
)

result = (
    valid
    .select("id", "store_id", "product_id", "quantity", "last_restock", "category")
    .orderBy(F.col("last_restock").desc())
)
