from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-04-24"))) & (F.col("quantity") != 97))
    .select(
        "*",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label_2"),
    )
)

scored = (
    filtered
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

valid = (
    scored
    .filter(F.col("seq_no") != 5)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock_label",
        "last_restock_label_2",
        "seq_no",
        (F.col("seq_no") + F.col("quantity")).alias("seq_no_plus_quantity"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        F.col("d.stock_qty"),
        F.col("d.product_name"),
        F.col("d.cost"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "stock_qty",
        "product_name",
        "cost",
        F.when(F.col("product_name").rlike("^(Desk Lamp|Backpack XL)$"), "GROUP_A").otherwise("PRIORITY").alias("product_name_zone"),
    )
    .orderBy("id")
)
