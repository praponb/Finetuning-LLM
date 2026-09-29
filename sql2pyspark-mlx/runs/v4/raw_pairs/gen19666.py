from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.when(F.col("store_name").rlike("^(Mall Central|Old Town)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("store_name_zone"),
    )
    .orderBy("id")
)
