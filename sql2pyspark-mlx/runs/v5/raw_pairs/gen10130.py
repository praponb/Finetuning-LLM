from pyspark.sql import functions as F

result = (
    df_inventory
    .select(F.lit("inventory").alias("kind"), F.col("last_restock").alias("event_date"))
    .unionByName(
        df_employees
        .select(F.lit("employee").alias("kind"), F.col("hire_date").alias("event_date"))
    )
    .orderBy(F.col("event_date").desc())
)
