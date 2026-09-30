from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("full_name").like("J%") & ~F.col("level").isin(["L2", "L3", "L1"])) | F.col("full_name").like("b%"))
    .select(
        "id",
        "full_name",
        F.when(F.col("level") == "L4", 1).otherwise(0).alias("is_l4"),
    )
)
