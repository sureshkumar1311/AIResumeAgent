"""
Azure Cosmos DB service for storing job descriptions and screening results
"""

from azure.cosmos import CosmosClient, PartitionKey, exceptions
from config import settings
from typing import Optional, List, Dict, Any
import uuid
from datetime import datetime


class CosmosDBService:
    """Service for Azure Cosmos DB operations"""
    
    def __init__(self):
        """Initialize Cosmos DB client"""
        self.client = CosmosClient(
            settings.COSMOS_DB_ENDPOINT,
            settings.COSMOS_DB_KEY
        )
        self.database = None
        self.jobs_container = None
        self.screenings_container = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database and containers"""
        try:
            # Create database if not exists
            self.database = self.client.create_database_if_not_exists(
                id=settings.COSMOS_DB_DATABASE_NAME
            )
            
            # Create jobs container if not exists
            self.jobs_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_JOBS,
                partition_key=PartitionKey(path="/job_id"),
                offer_throughput=400
            )
            
            # Create screenings container if not exists
            self.screenings_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_SCREENINGS,
                partition_key=PartitionKey(path="/job_id"),
                offer_throughput=400
            )
        
        except Exception as e:
            print(f"Error initializing Cosmos DB: {str(e)}")
            raise
    
    async def create_job_description(
        self,
        job_description_text: str,
        blob_url: str,
        must_have_skills: List[Dict],
        nice_to_have_skills: List[Dict],
        filename: str
    ) -> str:
        """
        Create a new job description entry
        
        Args:
            job_description_text: Extracted job description text
            blob_url: Azure Blob URL for the job description file
            must_have_skills: List of must-have skills
            nice_to_have_skills: List of nice-to-have skills
            filename: Original filename
        
        Returns:
            Job ID
        """
        try:
            job_id = str(uuid.uuid4())
            
            job_data = {
                "id": job_id,
                "job_id": job_id,
                "job_description_text": job_description_text,
                "blob_url": blob_url,
                "filename": filename,
                "must_have_skills": must_have_skills,
                "nice_to_have_skills": nice_to_have_skills,
                "created_at": datetime.utcnow().isoformat(),
                "total_screenings": 0,
                "status": "active"
            }
            
            self.jobs_container.create_item(body=job_data)
            return job_id
        
        except Exception as e:
            raise Exception(f"Failed to create job description in database: {str(e)}")
    
    async def get_job_description(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job description by ID
        
        Args:
            job_id: Job ID
        
        Returns:
            Job description data or None
        """
        try:
            item = self.jobs_container.read_item(
                item=job_id,
                partition_key=job_id
            )
            return item
        
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            raise Exception(f"Failed to retrieve job description: {str(e)}")
    
    async def update_job_screening_count(self, job_id: str):
        """
        Increment the screening count for a job
        
        Args:
            job_id: Job ID
        """
        try:
            job_data = await self.get_job_description(job_id)
            if job_data:
                job_data["total_screenings"] = job_data.get("total_screenings", 0) + 1
                job_data["last_screening_at"] = datetime.utcnow().isoformat()
                self.jobs_container.upsert_item(body=job_data)
        
        except Exception as e:
            print(f"Failed to update screening count: {str(e)}")
    
    async def save_screening_result(
        self,
        job_id: str,
        candidate_report: Dict[str, Any]
    ) -> str:
        """
        Save screening result for a candidate
        
        Args:
            job_id: Job ID
            candidate_report: Candidate screening report
        
        Returns:
            Screening result ID
        """
        try:
            screening_id = str(uuid.uuid4())
            
            screening_data = {
                "id": screening_id,
                "job_id": job_id,
                "screening_id": screening_id,
                "candidate_name": candidate_report.get("candidate_name"),
                "resume_url": candidate_report.get("resume_url"),
                "fit_score": candidate_report.get("fit_score"),
                "interview_worthy": candidate_report.get("interview_worthy"),
                "screening_details": candidate_report,
                "screened_at": datetime.utcnow().isoformat(),
                "status": "completed"
            }
            
            self.screenings_container.create_item(body=screening_data)
            
            # Update job screening count
            await self.update_job_screening_count(job_id)
            
            return screening_id
        
        except Exception as e:
            raise Exception(f"Failed to save screening result: {str(e)}")
    
    async def get_screening_results(
        self,
        job_id: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all screening results for a job
        
        Args:
            job_id: Job ID
            limit: Optional limit on number of results
        
        Returns:
            List of screening results
        """
        try:
            query = "SELECT * FROM c WHERE c.job_id = @job_id ORDER BY c.screened_at DESC"
            
            parameters = [{"name": "@job_id", "value": job_id}]
            
            items = list(self.screenings_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=job_id
            ))
            
            if limit:
                items = items[:limit]
            
            return items
        
        except Exception as e:
            raise Exception(f"Failed to retrieve screening results: {str(e)}")
    
    async def get_screening_by_id(
        self,
        screening_id: str,
        job_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get specific screening result
        
        Args:
            screening_id: Screening ID
            job_id: Job ID (partition key)
        
        Returns:
            Screening result or None
        """
        try:
            item = self.screenings_container.read_item(
                item=screening_id,
                partition_key=job_id
            )
            return item
        
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            raise Exception(f"Failed to retrieve screening result: {str(e)}")
    
    async def get_top_candidates(
        self,
        job_id: str,
        min_fit_score: int = 60,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top candidates for a job based on fit score
        
        Args:
            job_id: Job ID
            min_fit_score: Minimum fit score threshold
            limit: Maximum number of results
        
        Returns:
            List of top candidates
        """
        try:
            query = """
            SELECT * FROM c 
            WHERE c.job_id = @job_id 
            AND c.fit_score.score >= @min_score
            ORDER BY c.fit_score.score DESC
            """
            
            parameters = [
                {"name": "@job_id", "value": job_id},
                {"name": "@min_score", "value": min_fit_score}
            ]
            
            items = list(self.screenings_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=job_id
            ))
            
            return items[:limit]
        
        except Exception as e:
            raise Exception(f"Failed to retrieve top candidates: {str(e)}")
    
    async def delete_job_and_screenings(self, job_id: str) -> bool:
        """
        Delete job and all associated screening results
        
        Args:
            job_id: Job ID
        
        Returns:
            True if successful
        """
        try:
            # Delete all screening results
            screenings = await self.get_screening_results(job_id)
            for screening in screenings:
                self.screenings_container.delete_item(
                    item=screening["id"],
                    partition_key=job_id
                )
            
            # Delete job
            self.jobs_container.delete_item(
                item=job_id,
                partition_key=job_id
            )
            
            return True
        
        except Exception as e:
            print(f"Failed to delete job and screenings: {str(e)}")
            return False
    
    async def get_statistics(self, job_id: str) -> Dict[str, Any]:
        """
        Get statistics for a job's screening results
        
        Args:
            job_id: Job ID
        
        Returns:
            Statistics dictionary
        """
        try:
            screenings = await self.get_screening_results(job_id)
            
            if not screenings:
                return {
                    "total_screened": 0,
                    "average_fit_score": 0,
                    "interview_worthy_count": 0,
                    "interview_worthy_percentage": 0
                }
            
            total = len(screenings)
            fit_scores = [s["fit_score"]["score"] for s in screenings]
            interview_worthy = sum(1 for s in screenings if s["interview_worthy"])
            
            return {
                "total_screened": total,
                "average_fit_score": sum(fit_scores) / total if fit_scores else 0,
                "interview_worthy_count": interview_worthy,
                "interview_worthy_percentage": (interview_worthy / total * 100) if total > 0 else 0,
                "highest_fit_score": max(fit_scores) if fit_scores else 0,
                "lowest_fit_score": min(fit_scores) if fit_scores else 0
            }
        
        except Exception as e:
            raise Exception(f"Failed to calculate statistics: {str(e)}")