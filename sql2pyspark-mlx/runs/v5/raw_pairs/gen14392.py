from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-06-21"))) & F.col("product_id").isin([2, 8]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_sub("last_restock", 20).alias("last_restock_minus_20d"),
    )
)

result = (
    filtered
    .filter(F.col("product_id").isin([1, 4, 8]))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w_recent))
    .withColumn("prev_quantity_2", F.lag("quantity", 2).over(w_recent))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
)
