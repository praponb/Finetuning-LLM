from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "Kerry", "DHL"]) & F.col("carrier").isNull())
)

agg = (
    base
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
)

result = (
    agg
    .filter(F.col("unique_carriers") > 8)
    .select("status", "status_list", "unique_carriers")
    .orderBy("status")
)
