from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    ranked
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "row_num",
        F.floor("quantity").alias("quantity_whole"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.brand"))
)

result = (
    joined
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "row_num",
        "brand",
        F.when(F.col("brand").rlike("^(Initech|Globex)$"), "PRIORITY").otherwise("EXTENDED").alias("brand_zone"),
    )
    .orderBy(F.col("brand_zone").asc_nulls_first(), "row_num")
)
