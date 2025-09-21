from dotenv import load_dotenv
import os

# Explicitly load the .env file from the project root
load_dotenv(dotenv_path=".env")

print("SUPABASE_URL:", os.environ.get("SUPABASE_URL"))
print("SUPABASE_SERVICE_ROLE_KEY:", "set" if os.environ.get("SUPABASE_SERVICE_ROLE_KEY") else "missing")
