from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 69)
    .select("status", "carrier")
    .subtract(
        df_shipments
        .filter(F.month("shipped_date").isin([2, 7, 11]))
        .select("status", "carrier")
    )
    .orderBy("status")
)
