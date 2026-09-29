from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc_nulls_first(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-03-29").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
