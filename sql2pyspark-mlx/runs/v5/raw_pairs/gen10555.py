from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("id", "carrier")
    .intersectAll(
        df_shipments
        .filter(F.year("shipped_date") >= 2024)
        .select("id", "carrier")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = kept_rows.select("carrier", "delivered_date", "cost", "status")
