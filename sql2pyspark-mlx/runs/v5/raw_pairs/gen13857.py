from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.orderBy("prev_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("row_seq", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("prev_rating_avg_3")
w5 = Window.orderBy("last_known_prev_rating", "id")
w6 = Window.orderBy(F.col("last_known_prev_rating").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

metrics = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        F.lag("rating").over(w_grp).alias("prev_rating"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.last("prev_rating", ignorenulls=True).over(w2).alias("last_known_prev_rating"),
        F.avg("prev_rating").over(w3).alias("prev_rating_avg_3"),
    )
)

metrics_suppliers = (
    scored
    .filter(F.upper("supplier_name").like("%IAM%"))
    .select(
        "*",
        F.rank().over(w4).alias("prev_rating_avg_3_rank"),
        F.lag("prev_rating", 1).over(w5).alias("prev_prev_rating"),
        F.last("last_known_prev_rating", ignorenulls=True).over(w2).alias("last_known_last_known_prev_rating"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

with_latest = (
    metrics_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.cost").alias("product_cost"))
)

result = (
    with_latest
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("prev_rating_avg_3_rank", 0.95).alias("prev_rating_avg_3_rank_p95"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("prev_rating_avg_3_rank_p95").desc())
)
