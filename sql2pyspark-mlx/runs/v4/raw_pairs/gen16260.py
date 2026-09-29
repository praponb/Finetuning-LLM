from pyspark.sql import functions as F

src = df_shipments.filter(F.year("shipped_date") == 2023)

agg = (
    src
    .groupBy("status")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
)

result = (
    agg
    .filter(F.col("unique_carriers") < 10)
    .select("status", "stddev_cost", "unique_carriers")
    .orderBy(F.col("unique_carriers").desc(), F.col("status").desc())
)
