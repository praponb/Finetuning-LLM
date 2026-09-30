from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & ((F.col("carrier") != "DHL") | (F.col("cost") >= 27)))
)

summary = (
    base
    .groupBy(
        "status",
        F.when(F.col("carrier").isin(["Flash", "DHL"]), "tier_1").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.max("cost").alias("max_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = summary.orderBy("status", F.col("max_cost").desc())
