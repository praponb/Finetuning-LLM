from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select("id", "store_name", "region", "opened_date", "sqft")
    .orderBy(F.col("region").desc())
)
