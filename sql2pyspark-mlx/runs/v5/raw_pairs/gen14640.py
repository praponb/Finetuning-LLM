from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "product_id").orderBy(F.col("running_unit_price").desc_nulls_last(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())

scored = (
    df_order_items
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w)), 4))
    .withColumn("running_unit_price", F.sum("unit_price").over(w2))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

joined = (
    latest
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.cost"), F.col("p.product_name"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_share",
        "running_unit_price",
        "rank_in_group",
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
