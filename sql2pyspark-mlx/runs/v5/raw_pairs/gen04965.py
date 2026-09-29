from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("category").orderBy("is_grocery", "id")

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Stark"))))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        F.when(F.col("category") == "Grocery", 1).otherwise(0).alias("is_grocery"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity").between(194, 232)), "left_anti")
)

deduped = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
    .drop("latest_rank_2")
)

result = (
    ranked
    .select("id", "category", "cost", "stock_qty", "tags", "latest_rank")
    .orderBy("category")
)
