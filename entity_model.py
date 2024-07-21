from pydantic import BaseModel

class User(BaseModel):
    username: str = None # Make the username field optional
    email: str
    password: str
    token: str = None

class Db:
    db: str
    dbtype: str
    table: str
    tab_type: str
