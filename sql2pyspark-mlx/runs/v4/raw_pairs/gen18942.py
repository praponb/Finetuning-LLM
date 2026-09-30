from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-09-12").cast("date"))
    .withColumn("last_restock_day", F.dayofmonth("last_restock"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.price"), F.col("p.product_name"), F.col("p.category"))
)

remaining_keys = (
    with_dims
    .select("product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([3, 5, 6]))
        .select("product_id", "store_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "price",
        "product_name",
        "category",
        F.when(F.col("category").rlike("^(Grocery|Books)$"), "GROUP_B").when(F.col("category").rlike("^(Electronics|Home)$"), "PRIORITY").otherwise("EXTENDED").alias("category_zone"),
    )
    .orderBy(F.col("category").asc_nulls_first())
)
