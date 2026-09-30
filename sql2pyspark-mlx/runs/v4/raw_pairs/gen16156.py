from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]))
    .select("id", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") <= "2023-05-01")
        .select("id", "carrier")
    )
    .distinct()
)
