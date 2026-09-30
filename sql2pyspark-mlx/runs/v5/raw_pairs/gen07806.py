from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") == 2023)
    .select("shipped_date", "status")
    .subtract(
        df_shipments
        .filter(F.col("cost") >= 54)
        .select("shipped_date", "status")
    )
)
