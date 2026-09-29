from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter((F.col("i.quantity") < 204) & F.col("s.city").isin(["London", "Berlin", "Singapore"]))
    .select(
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
    .orderBy("store_name", "opened_date")
)
