from pydantic import BaseModel
from typing import Optional

class UserCreate(BaseModel):
    username: str
    password: str
    # Sử dụng kiểu cơ bản nhất để Pydantic không bị 'loạn'
    age: Optional[int] = 20 
    gender: Optional[str] = "Nam"

class UserResponse(BaseModel):
    user_id: int
    username: str
    
    class Config:
        # Pydantic v2 dùng from_attributes thay vì orm_mode
        from_attributes = True