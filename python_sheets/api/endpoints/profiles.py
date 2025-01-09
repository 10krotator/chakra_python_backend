from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from pydantic import BaseModel
from chakra_api.chakra_client import ChakraClient
import logging

router = APIRouter()

class ProfileResponse(BaseModel):
    profiles: List[Dict[str, Any]]
    count: int

@router.get("/profiles", response_model=ProfileResponse)
async def get_profiles(limit: int = 100):
    """
    Get profiles from database with limit
    """
    try:
        chakra = ChakraClient()
        df = chakra.query_data("linkedin_profiles", limit=limit)
        profiles = df.to_dict('records')

        return ProfileResponse(
            profiles=profiles,
            count=len(profiles)
        )

    except Exception as e:
        logging.error(f"Error in get_profiles endpoint: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch profiles: {str(e)}"
        )

@router.get("/search")
async def search_profiles(question: str):
    """
    Search profiles using natural language query
    """
    logging.info(f"Received search request with question: {question}")

    if not question.strip():
        raise HTTPException(
            status_code=400,
            detail="Search query cannot be empty"
        )

    try:
        chakra = ChakraClient()
        results, sql_query = chakra.execute_natural_query(question)
        return {
            "results": results.to_dict('records') if not results.empty else [],
            "count": len(results),
            "sql_query": sql_query
        }
    except Exception as e:
        logging.error(f"Error in search_profiles endpoint: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search profiles: {str(e)}"
        )