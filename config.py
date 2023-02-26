import os
# from dotenv import load_dotenv

# load_dotenv()

db_config = {
    "user"        : os.getenv('DB_USER'),
    "password"    : os.getenv('DB_PASSWORD'),
    "host"        : os.getenv('DB_HOST'),
    "database"    : os.getenv('DB_NAME'),
    "port"        : os.getenv('DB_PORT')
}

aws_s3_config = {
    "service_name"            :'s3',
    # "aws_access_key_id"       :os.getenv('AWS_ACCESS_KEY_ID'),
    # "aws_secret_access_key"   :os.getenv('AWS_SECRET_ACCESS_KEY')
}