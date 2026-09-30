from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(28, 37))
    .select("shipped_date")
    .unionByName(df_shipments.filter(F.col("cost") <= 53).select("shipped_date"))
    .distinct()
)
