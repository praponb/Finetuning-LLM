from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_whole").asc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_month_end_period", "id")
w3 = Window.partitionBy("store_id").orderBy("last_restock_month_end_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-02-07").cast("date"))
    .withColumn("quantity_whole", F.floor("quantity"))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 100), 2))
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "last_restock", "quantity_whole", "quantity_adjusted")
)

base = (
    ranked
    .filter(F.col("last_restock") > F.lit("2023-02-15").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity_whole",
        "quantity_adjusted",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

filtered_inventory = (
    base
    .filter(F.col("quantity_adjusted") >= 34)
    .withColumn("last_restock_month_end_period", F.date_trunc("MONTH", "last_restock_month_end"))
)

gaps = (
    filtered_inventory
    .withColumn("is_new_burst", F.when(F.col("last_restock_month_end_period") - F.lag("last_restock_month_end_period").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

result = (
    sessionized
    .select(
        "id",
        "quantity_adjusted",
        "last_restock_month_end",
        "is_new_burst",
        "session_no",
    )
    .orderBy(F.col("session_no").asc_nulls_first(), "is_new_burst")
)
