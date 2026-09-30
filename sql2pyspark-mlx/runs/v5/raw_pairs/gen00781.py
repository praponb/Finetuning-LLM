from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2024-04-01") | F.col("email").like("%@company.co"))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.sort_array(F.collect_set("segment")).alias("segment_list"))
    .orderBy(F.col("signup_year").desc())
)
