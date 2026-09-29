from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.age") != 54)
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("we.event_type") == "click", 1).otherwise(0)).alias("click_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.page")))).alias("page_values"),
    )
    .filter(F.col("click_count") > 54)
    .orderBy("page_values", "click_count")
)
