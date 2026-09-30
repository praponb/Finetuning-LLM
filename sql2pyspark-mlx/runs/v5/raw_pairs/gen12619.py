from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["DHL", "UPS"]), "tier_1").otherwise("other").alias("carrier_group"),
    )
    .agg(F.min("cost").alias("min_cost"))
    .filter(F.col("min_cost") >= 36)
    .select("carrier_group", "min_cost")
    .orderBy("carrier_group")
)
