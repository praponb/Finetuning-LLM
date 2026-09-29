from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    ranked
    .withColumn("running_quantity", F.sum("quantity").over(w2))
    .withColumn("running_quantity_2", F.sum("quantity").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w5))
)

valid = (
    scored
    .filter((F.col("last_restock") > F.lit("2023-09-13").cast("date")) & (F.col("event_seq") >= 7))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "running_quantity",
        "running_quantity_2",
        "first_known_quantity",
        (F.col("event_seq") - F.col("running_quantity_2")).alias("event_seq_minus_running_quantity_2"),
        F.date_add("last_restock", 21).alias("last_restock_plus_21d"),
    )
)

report = (
    valid
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "running_quantity_2",
        "first_known_quantity",
        "event_seq_minus_running_quantity_2",
        "last_restock_plus_21d",
    )
)

result = (
    report
    .select(
        "id",
        "product_id",
        "quantity",
        "running_quantity_2",
        "event_seq_minus_running_quantity_2",
        "last_restock_plus_21d",
    )
    .orderBy(F.col("id").desc())
)
