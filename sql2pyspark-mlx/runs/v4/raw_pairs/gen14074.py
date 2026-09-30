from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
)

result = (
    base
    .select(
        "*",
        F.rank().over(w).alias("cost_rank"),
        F.avg("cost").over(w2).alias("cost_avg_2_prior"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
    )
)
