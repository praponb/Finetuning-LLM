from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_plus_months").desc_nulls_last(), "id")

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-08-18").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

valid = (
    base
    .filter(F.col("product_id").isin([2, 5, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select("id", "quantity", "last_restock_plus_months")
    .orderBy(F.col("id").desc_nulls_last())
)
