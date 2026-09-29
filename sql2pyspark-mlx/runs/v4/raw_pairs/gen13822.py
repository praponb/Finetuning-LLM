from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([13, 17, 27, 31]))
    .groupBy(
        "status",
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy(F.col("status").desc())
)
