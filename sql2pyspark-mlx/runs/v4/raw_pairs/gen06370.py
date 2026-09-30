from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2025-03-01")
    .groupBy(
        F.when(F.col("carrier").isin(["Flash", "DHL"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("pct_of_total", F.round(((F.col("total_cost") * 100) / F.sum("total_cost").over(w)), 2))
    .orderBy("carrier_group", F.col("pct_of_total").desc())
    .limit(5)
)
