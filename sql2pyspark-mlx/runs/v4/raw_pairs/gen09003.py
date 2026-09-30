from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").asc_nulls_last())
w2 = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select("id", F.col("item.product_id"), F.col("item.qty"), F.col("item.promo"))
)

kept_keys = (
    flattened
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2025-03-01")
        .select("id")
    )
)

verified = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.dense_rank().over(w).alias("product_id_dense_rank"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
    )
)

result = (
    scored
    .select("product_id", "qty", "qty_share")
    .orderBy(F.col("qty").desc_nulls_last())
)
