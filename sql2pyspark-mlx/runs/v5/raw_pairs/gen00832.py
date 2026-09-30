from pyspark.sql import functions as F

result = (
    df_inventory
    .select("store_id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity") <= 209)
        .select("store_id")
    )
)
