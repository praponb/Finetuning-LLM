from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") >= 79)
    .drop("min_cost")
    .orderBy(F.col("cost_count").desc())
)
