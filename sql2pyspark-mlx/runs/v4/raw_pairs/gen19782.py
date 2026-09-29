from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "city")

scored = (
    df_customers
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.last("age", ignorenulls=True).over(w2).alias("last_known_age"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4).alias("credit_limit_share"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("credit_limit_share") >= 0.86, "HIGH").otherwise("LOW").alias("credit_limit_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.count("*").alias("event_count"),
    )
)
