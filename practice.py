
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
spark=SparkSession.builder.appName("practice").getOrCreate()
orders_schema=StructType(
    [
        StructField("order_id",IntegerType(),nullable=False),
        StructField("order_date",DateType()),
        StructField("order_customer_id",IntegerType(),nullable=False),
        StructField("order_status",StringType())
    ]
)

'''
customer_id: This column contains the id of the customer. Ex:- 1, 2, 3, etc.
customer_fname: This column has the customer’s first name details.
customer_lname: This column has the customer’s last name details.
customer_email: This column includes the customer’s email info.
customer_password: This column has customer password information. It’s encrypted.
customer_street: This has customer address-related info, which is street in this case.
customer_city: This has city-related information.
customer_state: The state info of the customer.
customer_zipcode: The zip code of the customer location.
'''
customers_schema=StructType(
    [
        StructField("customer_id",IntegerType(),nullable=False),
        StructField("customer_fname",StringType()),
        StructField("customer_lname",StringType()),
        StructField("customer_email",StringType()),
        StructField("customer_password",StringType()),
        StructField("customer_street",StringType()),
        StructField("customer_city",StringType()),
        StructField("customer_state",StringType()),
        StructField("customer_zipcode",IntegerType())
    ]
)

df_customers=spark.read.option("header","false").schema(customers_schema).csv("C:\\Users\\mishr\\Downloads\\"
                         "SPARK_TASKS-20250908T163711Z-1-001\\"
                         "SPARK_TASKS\\TASKS_DATASETS\\customers.csv")

df_customers.show()

df_orders=(spark.read.
            option("header","false").
            schema(orders_schema).
           csv("C:\\Users\\mishr\\Downloads\\"
                         "SPARK_TASKS-20250908T163711Z-1-001\\"
                         "SPARK_TASKS\\TASKS_DATASETS\\orders.csv"))

df_orders.show()




