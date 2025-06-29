
from typing import Any, Dict, Optional
import time
from functools import lru_cache
import json

import boto3
from jose import jwt, jwk, JWTError
from jose.utils import base64url_decode

from backend.core.config import get_settings
from backend.services.logging import get_logger

logger = get_logger(__name__)


class TokenVerifier:
    """JWT token verifier for AWS Cognito."""
    
    def __init__(self):
        self.settings = get_settings()
        self.region = self.settings.REGION
        self.user_pool_id = self.settings.COGNITO_POOL_ID
        self.client_id = self.settings.COGNITO_CLIENT_ID
        self.keys_url = f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}/.well-known/jwks.json"
        self._keys = None
        self._keys_timestamp = 0
        self._keys_ttl = 3600  # Cache keys for 1 hour
        
    @property
    def keys(self):
        """Get cached JWKS keys."""
        current_time = time.time()
        if self._keys is None or (current_time - self._keys_timestamp) > self._keys_ttl:
            self._refresh_keys()
        return self._keys
    
    def _refresh_keys(self):
        """Refresh JWKS keys from Cognito."""
        try:
            import urllib.request
            with urllib.request.urlopen(self.keys_url) as response:
                self._keys = json.loads(response.read())['keys']
                self._keys_timestamp = time.time()
                logger.info("Successfully refreshed Cognito JWKS keys")
        except Exception as e:
            logger.error(f"Failed to fetch JWKS keys: {str(e)}")
            raise JWTError(f"Failed to fetch JWKS keys: {str(e)}")
    
    def _get_key(self, kid: str):
        """Get specific key by kid."""
        for key in self.keys:
            if key['kid'] == kid:
                return key
        return None
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token.
        
        Args:
            token: JWT token string
            
        Returns:
            Dict[str, Any]: Decoded token claims
            
        Raises:
            JWTError: If token is invalid
        """
        try:
            # Get the kid from the headers
            headers = jwt.get_unverified_headers(token)
            kid = headers['kid']
            
            # Get the key
            key = self._get_key(kid)
            if not key:
                raise JWTError(f"Unable to find key with kid: {kid}")
            
            # Construct the public key
            public_key = jwk.construct(key)
            
            # Get the message and signature
            message, encoded_signature = token.rsplit('.', 1)
            decoded_signature = base64url_decode(encoded_signature.encode('utf-8'))
            
            # Verify the signature
            if not public_key.verify(message.encode('utf-8'), decoded_signature):
                raise JWTError('Invalid signature')
            
            # Decode the token
            claims = jwt.get_unverified_claims(token)
            
            # Verify claims
            current_time = time.time()
            
            # Check expiration
            if current_time > claims.get('exp', 0):
                raise JWTError('Token has expired')
            
            # Check issued at time
            if current_time < claims.get('iat', 0):
                raise JWTError('Token used before issued')
            
            # Check audience (client_id)
            if claims.get('client_id') != self.client_id and claims.get('aud') != self.client_id:
                raise JWTError('Invalid audience')
            
            # Check issuer
            expected_issuer = f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}"
            if claims.get('iss') != expected_issuer:
                raise JWTError('Invalid issuer')
            
            # Check token use
            token_use = claims.get('token_use')
            if token_use not in ['id', 'access']:
                raise JWTError('Invalid token use')
            
            logger.info(f"Successfully verified token for user: {claims.get('sub', 'unknown')}")
            return claims
            
        except JWTError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during token verification: {str(e)}")
            raise JWTError(f"Token verification failed: {str(e)}")


# Create singleton instance
_verifier = None


def get_verifier() -> TokenVerifier:
    """Get singleton TokenVerifier instance."""
    global _verifier
    if _verifier is None:
        _verifier = TokenVerifier()
    return _verifier


def verify_jwt(token: str) -> Dict[str, Any]:
    """Verify Cognito JWT token.
    
    Uses Cognito public key cache for verification.
    
    Args:
        token: JWT token string
        
    Returns:
        Dict[str, Any]: Decoded token claims including:
            - sub: User ID
            - email: User email (if present)
            - cognito:groups: User groups (if present)
            - exp: Expiration timestamp
            - iat: Issued at timestamp
        
    Raises:
        JWTError: If token is invalid
    """
    verifier = get_verifier()
    return verifier.verify_token(token)


def extract_user_info(claims: Dict[str, Any]) -> Dict[str, Any]:
    """Extract user information from JWT claims.
    
    Args:
        claims: Decoded JWT claims
        
    Returns:
        Dict[str, Any]: User information
    """
    return {
        'user_id': claims.get('sub'),
        'email': claims.get('email'),
        'email_verified': claims.get('email_verified', False),
        'groups': claims.get('cognito:groups', []),
        'username': claims.get('cognito:username', claims.get('username')),
        'token_use': claims.get('token_use'),
        'issued_at': claims.get('iat'),
        'expires_at': claims.get('exp')
    }


# FastAPI dependency
async def get_current_user(authorization: Optional[str] = None) -> Dict[str, Any]:
    """FastAPI dependency to get current user from JWT token.
    
    Args:
        authorization: Authorization header value
        
    Returns:
        Dict[str, Any]: User information
        
    Raises:
        HTTPException: If token is missing or invalid
    """
    from fastapi import HTTPException, status
    
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Extract token from "Bearer <token>" format
    try:
        scheme, token = authorization.split()
        if scheme.lower() != 'bearer':
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication scheme",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    try:
        claims = verify_jwt(token)
        return extract_user_info(claims)
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )