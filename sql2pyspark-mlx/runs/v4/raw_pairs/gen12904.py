from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("greatest_quantity_unit_price_filled").desc_nulls_first())
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("quantity") != 6)
    .select(
        "*",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

filtered = (
    valid
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low"))
    .withColumn("greatest_quantity_unit_price_filled", F.coalesce("greatest_quantity_unit_price", F.lit(0)))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
        F.sum("quantity").over(w3).alias("running_quantity"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("greatest_quantity_unit_price") >= 65, "PREMIUM").otherwise("STANDARD").alias("greatest_quantity_unit_price_tier"),
        "product_id",
    )
    .agg(
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.max("unit_price_share").alias("max_unit_price_share"),
    )
    .filter(F.col("max_unit_price_share") < 0)
)
