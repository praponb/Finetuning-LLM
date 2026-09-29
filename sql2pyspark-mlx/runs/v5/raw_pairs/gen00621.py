from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "LOST")
    .select("shipped_date")
    .unionByName(
        df_shipments
        .filter(F.year("shipped_date") >= 2024)
        .select("shipped_date")
    )
    .distinct()
)
