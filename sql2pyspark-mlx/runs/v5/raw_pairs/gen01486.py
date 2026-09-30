from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("brand", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id")

enriched = (
    df_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id").eqNullSafe(F.col("d.id")), "left")
    .select("s.*", F.col("d.cost"), F.col("d.brand"))
)

metrics = (
    enriched
    .filter(F.col("cost") >= 300)
    .select(
        "id",
        "unit_price",
        "brand",
        F.row_number().over(w).alias("seq_no"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
        F.lag("cost", 2, 0).over(w3).alias("prev_cost"),
    )
)

valid = (
    metrics
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "unit_price",
        "brand",
        "seq_no",
        "running_unit_price",
        "prev_cost",
        F.trim("brand").alias("clean_brand"),
        F.ceil("unit_price").alias("unit_price_whole"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

filtered = (
    valid
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Globex"))))
    .select(
        "*",
        F.when(F.col("brand") == "Initech", 1).otherwise(0).alias("is_initech"),
        F.when(F.col("running_unit_price") >= 1623, "High").when(F.col("running_unit_price") >= 659, "Medium").otherwise("Low").alias("running_unit_price_band"),
    )
)

result = (
    filtered
    .select(
        "id",
        "brand",
        "running_unit_price",
        "clean_brand",
        "unit_price_whole",
        "brand_clean",
        "is_initech",
        "running_unit_price_band",
        F.when(F.col("brand").rlike("^(Umbrella|Stark)$"), "CORE").when(F.col("brand").rlike("^(Acme|Globex)$"), "GROUP_B").otherwise("PRIORITY").alias("brand_zone"),
    )
    .orderBy("id")
)
