from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("kpi").orderBy(F.col("avg_value").desc())

metrics_long = (
    df_orders
    .select("id", "customer_id", "discount", "shipping_fee")
    .unpivot(
        ids=["id", "customer_id"],
        values=["discount", "shipping_fee"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
)

result = (
    metrics_long
    .groupBy(
        "kpi",
        F.when(F.col("value") >= 87, "LARGE").otherwise("SMALL").alias("value_tier"),
    )
    .agg(
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.size(F.collect_set("kpi")).alias("num_kpis"),
        F.percentile_approx("value", 0.5).alias("value_p50"),
    )
    .withColumn("avg_value_rank", F.rank().over(w))
    .orderBy(F.col("kpi").desc())
)
