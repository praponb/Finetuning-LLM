from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id")

deduped = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(F.when(F.col("cost") >= 95, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_per_delivered"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("cost_per_delivered") < 0)
    .orderBy("cost_stddev")
)
