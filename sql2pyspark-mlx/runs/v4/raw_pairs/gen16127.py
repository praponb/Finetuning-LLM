from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.device") == "tablet")
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.col("c.segment"),
    )
    .agg(
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
        F.sort_array(F.collect_set(F.col("s.country"))).alias("country_list"),
    )
    .orderBy(F.col("unique_countrys").desc(), F.col("signup_quarter").desc(), "segment")
    .limit(50)
)
