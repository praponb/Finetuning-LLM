from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("status") == "IN_TRANSIT")
        .select("id", "carrier")
    )
)
