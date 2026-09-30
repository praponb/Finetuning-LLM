from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "order_id",
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"))
    .filter(F.col("flash_count") >= 3)
    .orderBy("delivered_cohort", "flash_count")
)
