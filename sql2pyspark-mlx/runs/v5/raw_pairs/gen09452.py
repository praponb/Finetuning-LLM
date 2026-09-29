from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 1), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.when(F.col("store_name").rlike("^(Riverside|Old Town)$"), "PRIORITY").otherwise("CORE").alias("store_name_zone"),
    )
)
