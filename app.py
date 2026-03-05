import os
import sqlite3
import shutil
import hashlib
import jwt
from datetime import datetime, timedelta
from fastapi import FastAPI, File, UploadFile, HTTPException, Request, Depends, status
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import Dict, Any, List
import uvicorn

SECRET_KEY = "super-secret-key-for-sqlite-studio"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7 # 1 week

app = FastAPI(title="SQLite Studio Premium")
DATA_DIR = '.'
templates = Jinja2Templates(directory="templates")

# Setup Admin DB
ADMIN_DB = os.path.join(DATA_DIR, 'admin.db')
def init_admin_db():
    conn = sqlite3.connect(ADMIN_DB)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  username TEXT UNIQUE NOT NULL,
                  password TEXT NOT NULL)''')
    conn.commit()
    conn.close()

init_admin_db()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

class UserAuth(BaseModel):
    username: str
    password: str

class QueryRequest(BaseModel):
    query: str

class BulkDeleteRequest(BaseModel):
    rowids: List[int]

class BulkUpdateRequest(BaseModel):
    updates: List[Dict[str, Any]]

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials")
        return username
    except jwt.PyJWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials")

def get_user_dir(username: str):
    user_dir = os.path.join(DATA_DIR, username)
    os.makedirs(user_dir, exist_ok=True)
    return user_dir

@app.post("/api/register")
async def register_user(user: UserAuth):
    if not user.username or not user.password:
        raise HTTPException(status_code=400, detail="Missing username or password")
    
    conn = sqlite3.connect(ADMIN_DB)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, password) VALUES (?, ?)", (user.username, hash_password(user.password)))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=400, detail="Username already exists")
    conn.close()
    
    get_user_dir(user.username)
    return {"message": "User created successfully"}

@app.post("/api/login")
async def login_user(user: UserAuth):
    conn = sqlite3.connect(ADMIN_DB)
    c = conn.cursor()
    c.execute("SELECT password FROM users WHERE username = ?", (user.username,))
    row = c.fetchone()
    conn.close()
    
    if not row or row[0] != hash_password(user.password):
        raise HTTPException(status_code=401, detail="Incorrect username or password")
        
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer", "username": user.username}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/databases")
async def api_databases(username: str = Depends(get_current_user)):
    user_dir = get_user_dir(username)
    dbs = [f for f in os.listdir(user_dir) if f.endswith(('.db', '.sqlite', '.sqlite3'))]
    return {"databases": dbs}

def get_db_connection(username: str, db_name: str):
    user_dir = get_user_dir(username)
    db_path = os.path.join(user_dir, db_name)
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Database not found")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/database/{db_name}/tables")
async def api_tables(db_name: str, username: str = Depends(get_current_user)):
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row['name'] for row in cursor.fetchall()]
        conn.close()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/database/{db_name}/table/{table_name}")
async def api_table_data(db_name: str, table_name: str, username: str = Depends(get_current_user)):
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        columns = [row['name'] for row in cursor.fetchall()]
        
        cursor.execute(f"SELECT rowid as _rowid_, * FROM '{table_name}' LIMIT 100")
        rows = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return {"columns": columns, "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...), username: str = Depends(get_current_user)):
    if not file.filename.endswith(('.db', '.sqlite', '.sqlite3')):
        raise HTTPException(status_code=400, detail="Invalid file type. Only .db, .sqlite, .sqlite3 are allowed.")
    
    user_dir = get_user_dir(username)
    file_path = os.path.join(user_dir, file.filename)
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"message": f"Successfully uploaded {file.filename}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")

@app.post("/api/database/{db_name}/table/{table_name}/row")
async def api_create_row(db_name: str, table_name: str, data: Dict[str, Any], username: str = Depends(get_current_user)):
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        
        columns = list(data.keys())
        placeholders = ', '.join(['?'] * len(columns))
        values = list(data.values())
        
        query = f"INSERT INTO '{table_name}' ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.execute(query, values)
        
        conn.commit()
        lastrowid = cursor.lastrowid
        conn.close()
        return {"message": "Row added successfully", "rowid": lastrowid}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/api/database/{db_name}/table/{table_name}/row/{rowid}")
async def api_update_row(db_name: str, table_name: str, rowid: int, data: Dict[str, Any], username: str = Depends(get_current_user)):
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        
        update_data = {k: v for k, v in data.items() if k != '_rowid_'}
        if not update_data:
            raise HTTPException(status_code=400, detail="No data to update")
            
        columns = list(update_data.keys())
        set_clause = ', '.join([f"{col} = ?" for col in columns])
        values = list(update_data.values())
        values.append(rowid)
        
        query = f"UPDATE '{table_name}' SET {set_clause} WHERE rowid = ?"
        cursor.execute(query, values)
        
        conn.commit()
        rowcount = cursor.rowcount
        conn.close()
        
        if rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
            
        return {"message": "Row updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/api/database/{db_name}/table/{table_name}/rows/bulk_update")
async def api_bulk_update_rows(db_name: str, table_name: str, req: BulkUpdateRequest, username: str = Depends(get_current_user)):
    updates = req.updates
    if not updates:
        raise HTTPException(status_code=400, detail="No rows to update")
        
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        total_updated = 0
        
        for row_data in updates:
            rowid = row_data.pop('_rowid_', None)
            if rowid is None or not row_data:
                continue
                
            columns = list(row_data.keys())
            set_clause = ', '.join([f'"{col}" = ?' for col in columns])
            values = list(row_data.values())
            values.append(rowid)
            
            query = f'UPDATE "{table_name}" SET {set_clause} WHERE rowid = ?'
            cursor.execute(query, values)
            total_updated += cursor.rowcount
            
        conn.commit()
        conn.close()
        
        return {"message": f"Successfully updated {total_updated} rows"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/database/{db_name}/table/{table_name}/row/{rowid}")
async def api_delete_row(db_name: str, table_name: str, rowid: int, username: str = Depends(get_current_user)):
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        
        query = f"DELETE FROM '{table_name}' WHERE rowid = ?"
        cursor.execute(query, (rowid,))
        
        conn.commit()
        rowcount = cursor.rowcount
        conn.close()
        
        if rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
            
        return {"message": "Row deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/database/{db_name}/table/{table_name}/rows")
async def api_delete_rows(db_name: str, table_name: str, req: BulkDeleteRequest, username: str = Depends(get_current_user)):
    if not req.rowids:
        raise HTTPException(status_code=400, detail="No rows selected for deletion")
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?'] * len(req.rowids))
        query = f"DELETE FROM '{table_name}' WHERE rowid IN ({placeholders})"
        cursor.execute(query, req.rowids)
        
        conn.commit()
        rowcount = cursor.rowcount
        conn.close()
        
        return {"message": f"Successfully deleted {rowcount} rows"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/database/{db_name}")
async def api_delete_database(db_name: str, username: str = Depends(get_current_user)):
    try:
        user_dir = get_user_dir(username)
        db_path = os.path.join(user_dir, db_name)
        
        if not os.path.exists(db_path):
            raise HTTPException(status_code=404, detail="Database not found")
            
        os.remove(db_path)
        return {"message": f"Successfully deleted database {db_name}"}
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Failed to delete database: {str(e)}")

@app.post("/api/database/{db_name}/query")
async def api_query(db_name: str, data: QueryRequest, username: str = Depends(get_current_user)):
    query = data.query
    if not query:
        raise HTTPException(status_code=400, detail="Empty query")
        
    try:
        conn = get_db_connection(username, db_name)
        cursor = conn.cursor()
        cursor.execute(query)
        
        if query.strip().upper().startswith(('SELECT', 'PRAGMA', 'EXPLAIN')):
            columns = [col[0] for col in cursor.description] if cursor.description else []
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return {"columns": columns, "rows": rows}
        else:
            conn.commit()
            rowcount = cursor.rowcount
            conn.close()
            return {"message": f"Query executed successfully. Affected rows: {rowcount}"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/database/{db_name}/export")
async def api_export_database(db_name: str, username: str = Depends(get_current_user)):
    user_dir = get_user_dir(username)
    db_path = os.path.join(user_dir, db_name)
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Database not found")
    
    return FileResponse(path=db_path, filename=db_name, media_type='application/octet-stream')

if __name__ == '__main__':
    print("Starting Premium SQLite Studio with FastAPI...")
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
