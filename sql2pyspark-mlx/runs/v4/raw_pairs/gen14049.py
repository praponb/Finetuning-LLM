from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .intersectAll(
        df_shipments
        .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT"]))
        .select("status")
    )
)
