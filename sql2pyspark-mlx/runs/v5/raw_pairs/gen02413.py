from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-05-18").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.when(F.col("quantity") >= 207, "High").when(F.col("quantity") >= 70, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "last_restock_label")
)
