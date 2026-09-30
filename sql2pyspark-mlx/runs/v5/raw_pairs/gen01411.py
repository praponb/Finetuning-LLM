from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") > "2023-05-12") & ((F.col("status") == "RETURNED") | ~F.col("carrier").isin(["UPS", "Kerry"])))
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
