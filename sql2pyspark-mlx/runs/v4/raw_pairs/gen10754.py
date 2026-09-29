from pyspark.sql import functions as F

base = df_shipments.filter(F.col("shipped_date").between("2024-01-12", "2024-02-01"))

shipments_stats = (
    base
    .groupBy("carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    shipments_stats
    .select("carrier", "status_values")
    .orderBy("status_values")
)
