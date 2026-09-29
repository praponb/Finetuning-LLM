from pyspark.sql import functions as F

with_dims = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.sqft"), F.col("b.store_name"), F.col("b.region"))
)

result = (
    with_dims
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "sqft",
        "store_name",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Riverside)$"), "GROUP_A").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "EXTENDED").otherwise("PRIORITY").alias("store_name_zone"),
    )
    .orderBy("store_name_zone", F.col("quantity").desc())
)
