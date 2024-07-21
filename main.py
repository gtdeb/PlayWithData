import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import jwt
from jwt import encode as jwt_encode
from bson import ObjectId
from databases.mongo import users_collection
from dashboard import *
from Notification.sendEmail import sendEmail
from entity_model import User, Db
from operator import itemgetter
import os
from collections import OrderedDict

logging.basicConfig(level=logging.INFO)
app=FastAPI()
#router=APIRouter()

origins = [
    "http://localhost:3000",  # React development server
    "http://192.168.0.103:3000",  # If accessing from a specific IP
    # Add other origins as needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
 
# Secret key for signing the token
SECRET_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzg1MDg1NSwiaWF0IjoxNzE3ODUwODU1fQ.ZuUT0LHHEUwduX0JPig7xKELWCGZdnPm0uEk7jcDlXY"
security = HTTPBearer()

@app.get("/")
def homepage():
    return {"message": "Welcome to the homepage"}
    
@app.post("/login")
def login(user: User):
    # Check if user exists in the database
    user_data = users_collection.find_one({"email": user.email, "password": user.password})
    if user_data:
        # Generate a token
        #token = generate_token(user.email)
        # Convert ObjectId to string
        user_data["_id"] = str(user_data["_id"])
        # Store user details and token in local storage
        user_data["token"] = str(user_data["token"])
        return user_data
    return {"message": "Invalid email or password"}
    
@app.post("/register")
def register(user: User):
    # Check if user already exists in the database
    existing_user = users_collection.find_one({"email": user.email})
    if existing_user:
        return {"message": "User already exists"}
    # Generate a token
    token = generate_token(user.email)
    # Insert the new user into the database
    user_dict = user.dict()
    # Store user details and token in local storage
    user_dict["token"] = token
    res=users_collection.find_one(sort=[("id", pymongo.DESCENDING)])
    #maxrows=users_collection.count_documents({})
    print(res['id'])
    user_dict["id"] = str(int(res['id'])+1)
    users_collection.insert_one(user_dict)
    # Convert ObjectId to string
    user_dict["_id"] = str(user_dict["_id"])
    return user_dict
    
@app.get("/api/user")
def get_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    # Extract the token from the Authorization header
    token = credentials.credentials
    # Authenticate and retrieve the user data from the database based on the token
    # Here, you would implement the authentication logic and fetch user details
    # based on the token from the database or any other authentication mechanism
    # For demonstration purposes, assuming the user data is stored in local storage
    # Note: Local storage is not accessible from server-side code
    # This is just a placeholder to demonstrate the concept
    user_data = {
        "username": "John Doe",
        "email": "johndoe@example.com"
    }
    if user_data["username"] and user_data["email"]:
        return user_data
    raise HTTPException(status_code=401, detail="Invalid token")
    
def generate_token(email: str) -> str:
    payload = {"email": email}
    token = jwt_encode(payload, SECRET_KEY, algorithm="HS256")
    return token

def findUserByEmail(email: str):
    # Find user by email from database
    existing_user = users_collection.find_one({"email": email})
    if existing_user:
        return existing_user["username"]
    return None
   
def resetToken(email: str, token: str):
    filter = { 'email': email }
    newvalues = { "$set": { 'token': token } }
    users_collection.update_one(filter,newvalues)

@app.post("/forgotpassword")
async def forgotpassword(request: Request):
    data = await request.json()
    email=data['email']
    user_name=findUserByEmail(email)
    print(user_name)
    if user_name is not None:
        token = generate_token(email)
        print(token)
        resetToken(email, token)
        em=sendEmail()
        if em.send_email(email, token):
            return {"message": "Email sent successfully with password reset link!"}
        else:
            return {"message": "Failed to send email!"}
    else:
        return {"message": "Email does not exists in our database!"}

@app.post("/resetpassword")
async def resetpassword(request: Request):
    data = await request.json()
    print("Received data: ",data)
    _filter = {'token': data['token']}
    newpass = {"$set": {'password': data['newpass']}}
    print(_filter, newpass)
    users_collection.update_one(_filter,newpass)
    return {"message": "Password reset completed! Close the browser.", "token": data['token']}

@app.get("/getDetails_DB")
def getDetails_DB():
    db_import = {'id': 'import', 'name': 'Import', 'type': 'import'}
    db_mysql = getFrom_mysql()
    db_mongo = getfrom_mongo()
    databases = [{'id': 'database', 'name': 'Database', 'type': 'root', 'children': [db_import, db_mysql, db_mongo]}]
    return databases

@app.post("/fetchtabledata")
async def fetchtabledata(request: Request):
    data = await request.json()
    logging.info(f"Received request data: {data}")
    req = Db()
    req.db=data['db'];
    req.db_type=data['db_type']
    req.table=data['table']
    req.tab_type=data['tab_type']
    fetchCount=data['fetchCount']
    #print("Fetch Count: " + str(fetchCount));
    
    if not req.db or not req.db_type or not req.table or not req.tab_type:
        raise HTTPException(status_code=422, detail="Invalid input data")
    else:
        data_dict=dict()
        try:
            if(req.db_type == 'mysql'):
                data_dict=readAndsendTableData_mysql(req, fetchCount)
            elif(req.db_type == 'mongo'):
                data_dict=readAndsendTableData_mongo(req, fetchCount)
        except:
            # Your logic to pull table data
            return {"message": "Data fetch unsuccessfull!"}
        else:
            #print(data_dict)
            return(data_dict)

@app.post("/fetchsortedtabledata")
async def fetchsortedtabledata(request: Request):
    data = await request.json()
    logging.info(f"Received request data: {data}")
    req = Db()
    req.db=data['db'];
    req.db_type=data['db_type']
    req.table=data['table']
    req.tab_type=data['tab_type']
    sort_model = data.get("sortModel", [])
    print("sorted model: "+ ' '.join(sort_model))
    data_dict=dict()
    try:
        if(req.db_type == 'mysql'):
            data_dict=readAndsendTableData_mysql(req)
        elif(req.db_type == 'mongo'):
            data_dict=readAndsendTableData_mongo(req)
    except:
        # Your logic to pull table data
        return {"message": "Data fetch unsuccessfull!"}
    else:
        print(data_dict)
        try:
            # Sort the data based on the sort model
            newlist = dict()
            for sort_item in sort_model:
                newlist = sorted(data_dict, key=itemgetter(sort_item['field']), reverse=(sort_item['sort'] == 'desc'))
                '''
                data_dict.sort(
                    key=lambda x: x[sort_item['field']],
                    reverse=(sort_item['sort'] == 'desc')
                )
                '''
            json_compatible_item_data = jsonable_encoder(newlist)
            print(newlist)
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise HTTPException(status_code=500, detail="Error fetching table data")
        else:
            print("sorted data getting returned from the server...")
            return JSONResponse(content=json_compatible_item_data)

@app.post("/getMaxRowId")
async def getMaxRowId(request: Request):
    data = await request.json()
    req = Db()
    req.db=data['db'];
    req.db_type=data['db_type']
    req.table=data['table']
    req.tab_type=data['tab_type']
    maxid=0
    
    if not req.db or not req.db_type or not req.table or not req.tab_type:
        raise HTTPException(status_code=422, detail="Invalid input data")
    else:
        try:
            if(req.db_type == 'mysql'):
                maxid=maxRowId_table_mysql(req)
            elif(req.db_type == 'mongo'):
                maxid=maxRowId_collection_mongo(req)
        except:
            # Your logic to pull table data
            return {"message": "Max Row id query unsuccessfull!"}
        else:
            return(maxid)

@app.post("/inserttabledata")
async def inserttabledata(request: Request):
    data = await request.json()
    req = Db()
    req.db=data['db'];
    req.db_type=data['db_type']
    req.table=data['table']
    req.tab_type=data['tab_type']
    addBuf=data['addBuf']
    status="";
    #print(addBuf)
    if not req.db or not req.db_type or not req.table or not req.tab_type:
        raise HTTPException(status_code=422, detail="Invalid input data")
    else:
        try:
            if(req.db_type == 'mysql'):
                status=insertTableData_mysql(req, addBuf)
            elif(req.db_type == 'mongo'):
                status=insertTableData_mongo(req, addBuf)
        except:
            # Your logic to pull table data
            return {"message": "Data insert unsuccessfull!"}
        else:
            return(status)

@app.post("/updatetabledata")
async def updatetabledata(request: Request):
    data = await request.json()
    req = Db()
    req.db=data['db'];
    req.db_type=data['db_type']
    req.table=data['table']
    req.tab_type=data['tab_type']
    dirtyBuf=data['dirtyBuf']
    status="";
    if not req.db or not req.db_type or not req.table or not req.tab_type:
        raise HTTPException(status_code=422, detail="Invalid input data")
    else:
        try:
            if(req.db_type == 'mysql'):
                status=updateTableData_mysql(req, dirtyBuf)
            elif(req.db_type == 'mongo'):
                status=updateTableData_mongo(req, dirtyBuf)
        except:
            # Your logic to pull table data
            return {"message": "Data update unsuccessfull!"}
        else:
            return(status)

@app.post("/deletetabledata")
async def deletetabledata(request: Request):
    data = await request.json()
    req = Db()
    req.db=data['db'];
    req.db_type=data['db_type']
    req.table=data['table']
    req.tab_type=data['tab_type']
    delBuf=data['delBuf']
    status="";

    if not req.db or not req.db_type or not req.table or not req.tab_type:
        raise HTTPException(status_code=422, detail="Invalid input data")
    else:
        try:
            if(req.db_type == 'mysql'):
                status=deleteTableData_mysql(req, delBuf)
            elif(req.db_type == 'mongo'):
                status=deleteTableData_mongo(req, delBuf)
        except:
            # Your logic to pull table data
            return {"message": "Data delete unsuccessfull!"}
        else:
            return(status)

@app.get("/loadfilestoimport")
def loadfilestoimport():
    path = "./datasets"
    dir_list = os.listdir(path)
    return dir_list

@app.post("/loadfiledata")
async def loadfiledata(request: Request):
    data = await request.json()
    absfilepath="./datasets/" + data['file']
    #print(absfilepath)
    df=pd.read_csv(absfilepath, nrows=5)
    df = df.replace(np.nan, "Unknown")
    data_list=[]
    for index in range(0,len(df)):
        row=df.iloc[index].to_dict()
        ordered_row = OrderedDict([('id', index + 1)])
        ordered_row.update(row)
        data_list.append(ordered_row)
    return data_list

@app.post("/importdataintotable")
async def importdataintotable(request: Request):
    data = await request.json()
    database=data['database']
    selectedFile=data['selectedFile']
    tableName=data['tableName']
    vColumns=data['visibleColumns']
    schemaData=data['schemaData']
    insert_response="";
    
    if database == "mysqldb":
        insert_response=importdatamysql(selectedFile, tableName, vColumns, schemaData)
    else:
        insert_response=importdatamongo(selectedFile, tableName, vColumns)
    return insert_response

@app.post("/dropoperation")
async def dropoperation(request: Request):
    data = await request.json()
    #print(data)
    selectedNode=data['selectedNode']
    
    if selectedNode['type'] == "table":
        dropstatus=droptablemysql(selectedNode)
    else:
        dropstatus=dropcollectionmongo(selectedNode)
    #print(dropstatus)
    if dropstatus == 1:
        return {'message': selectedNode['name']+' dropped successfully'}
    else:
        return {'message': "Can't drop "+selectedNode['name']}

@app.post("/generate_graph")
async def generate_graph(request: Request):
    data = await request.json()
    try:
        graph_data = plot_graph(data)
        return {'graph': graph_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
