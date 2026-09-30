from pyspark.sql import functions as F

result = (
    df_orders
    .cube(
        "channel",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.min("order_date").alias("first_order_date"))
)
