from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
    )
    .filter(F.col("smb_credit_limit") > 22)
    .drop("smb_credit_limit")
)
