from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & (F.col("shipped_date") <= "2025-02-01"))
    .groupBy("carrier", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
    .orderBy("carrier_values")
)
