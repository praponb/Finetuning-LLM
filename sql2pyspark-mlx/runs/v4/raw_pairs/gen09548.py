from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2023-07-01")
    .withColumn("status_group", F.when(F.col("status").isin(["EXPIRED", "PAUSED"]), "Group A").otherwise("Other"))
)
