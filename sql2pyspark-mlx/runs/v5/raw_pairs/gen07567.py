from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("email").like("%@GMAIL.COM") & F.col("is_active"))
    .groupBy("city", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("signup_quarter").desc(), "city")
    .limit(3)
)
