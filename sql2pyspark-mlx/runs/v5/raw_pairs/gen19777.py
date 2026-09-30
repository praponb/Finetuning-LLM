from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())
w2 = Window.orderBy(F.col("quantity_rounded_minus_quantity").desc())
w3 = Window.orderBy("quantity_rounded_minus_quantity")
w4 = Window.orderBy(F.col("quantity").desc_nulls_first())
w5 = Window.orderBy(F.col("quantity_rounded_minus_quantity").desc_nulls_first())

latest = (
    df_inventory
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "quantity", "last_restock")
)

filtered = (
    latest
    .filter(F.col("last_restock") > "2023-07-01")
    .withColumn("quantity_rounded", F.round("quantity", 1))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") <= 70)
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_inventory = (
    reconciled
    .filter(F.col("last_restock") >= F.lit("2023-07-20").cast("date"))
    .select(
        "id",
        "quantity",
        "last_restock_plus_months",
        (F.col("quantity_rounded") - F.col("quantity")).alias("quantity_rounded_minus_quantity"),
    )
)

with_windows = (
    filtered_inventory
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "quantity",
        F.percent_rank().over(w2).alias("quantity_rounded_minus_quantity_percent_rank"),
        F.percent_rank().over(w3).alias("quantity_rounded_minus_quantity_percent_rank_2"),
        F.cume_dist().over(w4).alias("quantity_cume_dist"),
        F.cume_dist().over(w5).alias("quantity_rounded_minus_quantity_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "quantity",
        "quantity_rounded_minus_quantity_percent_rank",
        "quantity_rounded_minus_quantity_percent_rank_2",
    )
)
