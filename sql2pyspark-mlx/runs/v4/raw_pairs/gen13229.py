from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("signup_date", "id")
w4 = Window.partitionBy("country", "city")
w_hist = Window.orderBy("signup_date", "id")
w6 = Window.partitionBy("country")
w_recent = Window.partitionBy("country").orderBy("seq_no_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("city", "segment").orderBy(F.col("country_avg_seq_no").desc())

metrics = (
    df_customers
    .select(
        "*",
        F.first("city", ignorenulls=True).over(w).alias("first_known_city"),
        F.sum("age").over(w2).alias("cumulative_age"),
        F.row_number().over(w3).alias("seq_no"),
        F.round((F.col("age") / F.sum("age").over(w4)), 4).alias("age_share"),
        F.lead("credit_limit", 2).over(w_hist).alias("next_credit_limit"),
    )
)

metrics_customers = (
    metrics
    .select(
        "*",
        F.avg("seq_no").over(w6).alias("country_avg_seq_no"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w6)), 4).alias("seq_no_share"),
    )
)

enriched = (
    metrics_customers
    .withColumn("cumulative_age_2", F.sum("age").over(w_recent))
    .withColumn("country_avg_seq_no_percent_rank", F.percent_rank().over(w_ordered))
)

result = (
    enriched
    .groupBy("segment")
    .pivot("city", ["Singapore", "Bangkok"])
    .agg(F.max("age_share"))
)
