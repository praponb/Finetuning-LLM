from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["Flash", "Kerry"]), "group_a").otherwise("other").alias("carrier_group"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") > 160)
    .orderBy("carrier_group", "total_cost")
)
