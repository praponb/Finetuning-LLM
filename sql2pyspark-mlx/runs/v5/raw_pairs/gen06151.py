from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("last_restock_quarter").desc(), "id")
w3 = Window.orderBy("last_restock_quarter_rounded", "id")
w4 = Window.orderBy("first_known_quantity_filled", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-08").cast("date"))
    .withColumn("last_restock_plus_25d", F.date_add("last_restock", 25))
    .withColumn("last_restock_quarter", F.quarter("last_restock"))
)

valid_inventory = (
    valid
    .filter(F.col("last_restock_quarter") < 43)
    .select(
        "id",
        "last_restock",
        "last_restock_plus_25d",
        "last_restock_quarter",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round("last_restock_quarter", 2).alias("last_restock_quarter_rounded"),
    )
)

with_windows = (
    valid_inventory
    .select(
        "*",
        F.first("quantity_filled", ignorenulls=True).over(w).alias("first_known_quantity_filled"),
        F.last("quantity_filled", ignorenulls=True).over(w).alias("last_known_quantity_filled"),
        F.first("last_restock_quarter", ignorenulls=True).over(w).alias("first_known_last_restock_quarter"),
        F.ntile(5).over(w2).alias("last_restock_quarter_bucket"),
        F.ntile(4).over(w3).alias("last_restock_quarter_rounded_bucket"),
    )
)

enriched = (
    with_windows
    .filter(F.col("last_restock_quarter") > 44)
    .withColumn("last_restock_quarter_bucket_2", F.ntile(3).over(w2))
    .withColumn("first_known_quantity_filled_bucket", F.ntile(10).over(w4))
)

result = (
    enriched
    .select(
        "id",
        "last_restock",
        "last_restock_plus_25d",
        "last_restock_quarter",
        "last_restock_quarter_rounded",
        "first_known_quantity_filled",
        "first_known_last_restock_quarter",
        "last_restock_quarter_bucket",
        "last_restock_quarter_rounded_bucket",
    )
    .orderBy(F.col("last_restock_quarter_rounded").desc())
)
