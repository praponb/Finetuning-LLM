from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("status") == "PAUSED")

status_group_summary = (
    src
    .groupBy(
        F.when(F.col("status").isin(["ACTIVE", "EXPIRED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
)

result = status_group_summary
