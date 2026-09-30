from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("region").orderBy(F.col("row_seq").desc(), "id")
w4 = Window.partitionBy("city").orderBy("sqft_share", "id")
w_grp = Window.partitionBy("region").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("next_sqft", "id")
w7 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("region").orderBy("next_next_sqft", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

metrics = (
    df_stores
    .withColumn("next_sqft", F.lead("sqft").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
)

enriched = (
    metrics
    .withColumn("row_seq_bucket", F.ntile(3).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("running_row_seq", F.sum("row_seq").over(w_grp))
    .withColumn("next_next_sqft", F.lead("next_sqft", 1).over(w6))
)

enriched_stores = (
    enriched
    .select(
        "*",
        F.sum("event_seq").over(w7).alias("running_event_seq"),
        F.avg("next_next_sqft").over(w8).alias("next_next_sqft_avg_2_prior"),
        F.avg("sqft").over(w9).alias("sqft_avg_6"),
    )
)

with_latest = (
    enriched_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"))
)

result = (
    with_latest
    .select("id", "city", "sqft_share", "shipping_fee")
    .unpivot(
        ids=["id", "city"],
        values=["sqft_share", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
