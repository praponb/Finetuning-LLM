from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("order_id", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("delivered_date") >= "2024-11-14")
        .select("order_id", "id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("order_id", "carrier", "status")
