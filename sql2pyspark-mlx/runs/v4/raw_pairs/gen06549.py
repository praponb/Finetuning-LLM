from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") >= 1110) & F.col("shipping_fee").between(15, 18) & (F.col("shipping_fee") >= 25))
    .groupBy(
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.when(F.col("amount") >= 1340, "high").when(F.col("amount") >= 330, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.countDistinct("amount").alias("unique_statuss"),
        F.count("*").alias("row_count"),
    )
)
