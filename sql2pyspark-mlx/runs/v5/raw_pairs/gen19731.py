from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element_int_2").asc_nulls_last())
w2 = Window.partitionBy("plan")

lines = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("element"))

valid = (
    lines
    .filter(F.col("element") >= 45)
    .withColumn("element_int", F.col("element").cast("int"))
    .withColumn("element_int_2", F.col("element").cast("int"))
)

base = (
    valid
    .filter(F.col("element_int").between(53, 58))
    .select(
        "id",
        "plan",
        "element_int",
        "element_int_2",
        F.col("element_int_2").cast("string").alias("element_int_2_num"),
    )
)

scored = (
    base
    .select(
        "id",
        "element_int",
        "element_int_2_num",
        F.cume_dist().over(w).alias("element_int_2_cume_dist"),
        F.avg("element_int_2").over(w2).alias("plan_avg_element_int_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("plan_avg_element_int_2").between(59, 78))
    .select(
        "*",
        F.least("element_int", "plan_avg_element_int_2").alias("least_element_int_plan_avg_element_int_2"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "element_int_2_num",
        "element_int_2_cume_dist",
        "plan_avg_element_int_2",
        "least_element_int_plan_avg_element_int_2",
    )
    .orderBy(F.col("id").desc())
)
