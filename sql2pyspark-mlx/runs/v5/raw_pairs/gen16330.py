from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 43)
    .select(
        "name",
        "email",
        F.length("name").alias("name_length"),
        F.when(F.col("segment") == "SMB", 1).otherwise(0).alias("is_smb"),
    )
)
