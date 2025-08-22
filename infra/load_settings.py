import os
from pathlib import Path
from dotenv import load_dotenv

class env_settings:
  def __init__(self, database_type: str):
      self.database_type = database_type

  def load_settings(self):
    """
    Load settings with environment variables
    """
    database = self.database_type
    dotenv_path = Path.cwd() / '.env'
    load_dotenv(dotenv_path=dotenv_path)

    settings = {
        "db_host": os.getenv(f'{database}_HOST'),
        "db_user": os.getenv(f'{database}_USERNAME'),
        "db_pass": os.getenv(f'{database}_PASSWORD'),
        "db_service": os.getenv(f'{database}_SERVICE'),
        "db_port": os.getenv(f'{database}_PORT'),
        "db_driver": os.getenv(f'{database}_DRIVER'),
    }
    return settings