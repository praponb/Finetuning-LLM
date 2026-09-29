from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-11-01")
    .select("shipped_date", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("status") != "LOST")
        .select("shipped_date", "carrier")
    )
    .distinct()
    .orderBy("shipped_date")
)
