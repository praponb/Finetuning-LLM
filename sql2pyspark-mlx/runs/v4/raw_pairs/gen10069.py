from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_plus_months").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock_plus_months").desc(), "id")
w3 = Window.orderBy("last_restock_plus_months", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("seq_no", "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("product_id").isin([6, 8, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
)

enriched = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("seq_no_2", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no_3", F.row_number().over(w3))
    .withColumn("seq_no_4", F.row_number().over(w3))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.product_name"), F.col("b.launch_date"))
)

result = (
    joined
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock_plus_months",
        "seq_no",
        "row_seq",
        "seq_no_3",
        "seq_no_4",
        "dedup_rank",
        "product_name",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("seq_no_4").desc_nulls_first())
)
