"""Q06b — LEFT ANTI join (reference conversion)."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build(spark: SparkSession) -> DataFrame:
    c, o = spark.table("customers"), spark.table("orders")
    return (
        c.join(o, "customer_id", "left_anti")
         .where(F.col("signup_date") < F.lit("2024-01-01").cast("date"))
         .select(
             "customer_id", "full_name", "signup_date",
             F.size(F.map_keys("preferences")).alias("n_prefs"),
         )
    )
