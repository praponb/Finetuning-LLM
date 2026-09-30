from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 6, 10]) & (F.col("status") != "CANCELLED"))
)

agg = (
    src
    .groupBy(
        F.month("end_date").alias("end_month"),
        F.when(F.col("status").isin(["CANCELLED", "EXPIRED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    agg
    .select("end_month", "status_group", "status_values", "status_list")
    .orderBy(F.col("end_month").desc())
)
