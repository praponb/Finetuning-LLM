from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "store_name",
        "opened_date",
        F.when(F.col("store_name").rlike("^(Downtown Express|Siam Flagship)$"), "CORE").otherwise("OTHER_ZONE").alias("store_name_zone"),
    )
)
