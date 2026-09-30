from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2024-01-19").cast("date")) & F.col("quantity").between(172, 230))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 266, "High").when(F.col("quantity") >= 38, "Medium").otherwise("Low"))
    .withColumn("last_restock_plus_52d", F.date_add("last_restock", 52))
)

filtered_inventory = (
    filtered
    .filter((F.col("last_restock") >= F.lit("2023-03-06").cast("date")) & (F.col("last_restock_plus_52d") <= "2024-12-01"))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
)

latest = (
    filtered_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 193, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quantity_tier", "product_id"], ["quantity_tier"], []],
        "quantity_tier",
        "product_id",
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)
