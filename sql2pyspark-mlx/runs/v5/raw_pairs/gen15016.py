from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]))
    .groupBy(
        "status",
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.min("cost").alias("min_cost"))
)
