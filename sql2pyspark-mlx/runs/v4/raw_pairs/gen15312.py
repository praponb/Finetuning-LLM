from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"), F.col("c.amount"))
)

result = (
    latest_match
    .withColumn("amount_tier", F.when(F.col("amount") >= 1570, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_order_date"))
    .groupingSets(
        [["city", "amount_tier", "quarter_start"], ["city", "amount_tier"], ["city"]],
        "city",
        "amount_tier",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .orderBy(F.col("amount_tier").desc_nulls_last())
)
