from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("end_date").desc())

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") > F.lit("2023-03-17").cast("date")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.length("plan").alias("plan_length"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
