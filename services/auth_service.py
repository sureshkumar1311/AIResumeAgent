"""
Authentication service for user management and JWT token handling
"""

from datetime import datetime, timedelta
from typing import Optional, Dict
import bcrypt
from jose import JWTError, jwt
from config import settings


class AuthService:
    """Service for authentication and authorization"""
    
    def __init__(self):
        """Initialize authentication service"""
        pass
    
    def hash_password(self, password: str) -> str:
        """
        Hash a password using bcrypt
        
        Args:
            password: Plain text password
        
        Returns:
            Hashed password
        """
        # Convert password to bytes and truncate to 72 bytes
        password_bytes = password.encode('utf-8')[:72]
        
        # Generate salt and hash
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password_bytes, salt)
        
        # Return as string
        return hashed.decode('utf-8')
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """
        Verify a password against its hash
        
        Args:
            plain_password: Plain text password
            hashed_password: Hashed password from database
        
        Returns:
            True if password matches, False otherwise
        """
        try:
            # Convert to bytes and truncate to 72 bytes
            password_bytes = plain_password.encode('utf-8')[:72]
            hashed_bytes = hashed_password.encode('utf-8')
            
            # Verify password
            return bcrypt.checkpw(password_bytes, hashed_bytes)
        except Exception as e:
            print(f"Password verification error: {str(e)}")
            return False
    
    def create_access_token(
        self,
        data: Dict,
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Create JWT access token
        
        Args:
            data: Data to encode in token (typically user_id and email)
            expires_delta: Optional custom expiration time
        
        Returns:
            Encoded JWT token
        """
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(
                minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES
            )
        
        to_encode.update({"exp": expire})
        
        encoded_jwt = jwt.encode(
            to_encode,
            settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM
        )
        
        return encoded_jwt
    
    def decode_access_token(self, token: str) -> Optional[Dict]:
        """
        Decode and validate JWT token
        
        Args:
            token: JWT token string
        
        Returns:
            Decoded payload if valid, None otherwise
        """
        try:
            payload = jwt.decode(
                token,
                settings.JWT_SECRET_KEY,
                algorithms=[settings.JWT_ALGORITHM]
            )
            return payload
        
        except JWTError:
            return None