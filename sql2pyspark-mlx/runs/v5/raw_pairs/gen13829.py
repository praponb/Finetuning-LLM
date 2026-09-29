from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)
