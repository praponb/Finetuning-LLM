from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("unit_price_int", "id")
w3 = Window.orderBy(F.col("unit_price_int").desc_nulls_last())
w_grp = Window.orderBy(F.col("unit_price_int").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("order_id") == 28)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.round("quantity", 2).alias("quantity_rounded"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

valid = (
    filtered
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").between(310, 370))
    .select(
        "id",
        "unit_price",
        "unit_price_int",
        F.when(F.col("unit_price") >= 350, "High").when(F.col("unit_price") >= 190, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
)

with_windows = (
    valid
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w_recent))
    .withColumn("first_known_unit_price_2", F.first("unit_price", ignorenulls=True).over(w_recent))
    .withColumn("next_unit_price_int", F.lead("unit_price_int", 1, 0).over(w_running))
    .withColumn("unit_price_int_rank", F.rank().over(w3))
    .withColumn("unit_price_int_bucket", F.ntile(10).over(w_grp))
)

result = (
    with_windows
    .select(
        "id",
        "unit_price",
        "unit_price_int",
        "unit_price_tier",
        "next_unit_price_int",
        "unit_price_int_rank",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
