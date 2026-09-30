from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(30, 33) & (F.col("discount") <= 0.37))
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .filter(F.col("returned_count") > 62)
    .orderBy(F.col("order_cohort").desc())
)
