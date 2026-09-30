from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city")
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("age", "id")

scored = (
    df_customers
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.first("segment", ignorenulls=True).over(w2).alias("first_known_segment"),
        F.ntile(5).over(w3).alias("age_bucket"),
    )
)

result = (
    scored
    .groupBy("city")
    .agg(
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("city").asc_nulls_last())
)
