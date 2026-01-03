from mangum import Mangum
from main import app  # because main.py has: app = FastAPI(...)

handler = Mangum(app)
