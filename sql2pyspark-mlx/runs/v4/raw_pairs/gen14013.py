from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_last())
w2 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_customers
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("credit_limit_cume_dist"),
        F.first("age", ignorenulls=True).over(w2).alias("first_known_age"),
    )
)

result = (
    with_windows
    .groupBy("city")
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.avg(F.when(F.col("city") == "Singapore", F.col("credit_limit"))).alias("singapore_avg_credit_limit"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.round(F.avg("credit_limit_cume_dist"), 2).alias("avg_credit_limit_cume_dist"),
    )
    .filter(F.col("avg_credit_limit_cume_dist") >= 0)
)
