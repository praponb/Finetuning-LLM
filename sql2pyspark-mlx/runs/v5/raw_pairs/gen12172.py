from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .filter(~F.col("p.city").isin(["Bangkok", "Chiang Mai", "New York"]))
    .select(
        "a.id",
        "a.store_id",
        "a.units",
        "a.revenue",
        F.col("p.store_name"),
        F.col("p.city"),
        F.col("p.opened_date"),
    )
)
