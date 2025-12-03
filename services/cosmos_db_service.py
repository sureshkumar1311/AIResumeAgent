"""
Azure Cosmos DB service for storing job descriptions, screening results, and users
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
        self.users_container = None
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
                partition_key=PartitionKey(path="/user_id"),
                offer_throughput=400
            )
            
            # Create screenings container if not exists
            self.screenings_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_SCREENINGS,
                partition_key=PartitionKey(path="/job_id"),
                offer_throughput=400
            )
            
            # Create users container if not exists
            self.users_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_USERS,
                partition_key=PartitionKey(path="/user_id"),
                offer_throughput=400
            )
        
        except Exception as e:
            print(f"Error initializing Cosmos DB: {str(e)}")
            raise
    
    # ==================== USER MANAGEMENT ====================
    
    async def create_user(
        self,
        email: str,
        hashed_password: str,
        full_name: str,
        company_name: Optional[str] = None
    ) -> str:
        """
        Create a new user
        
        Args:
            email: User email (unique identifier)
            hashed_password: Hashed password
            full_name: Full name of user
            company_name: Optional company name
        
        Returns:
            User ID
        """
        try:
            user_id = str(uuid.uuid4())
            
            user_data = {
                "id": user_id,
                "user_id": user_id,
                "email": email.lower(),
                "hashed_password": hashed_password,
                "full_name": full_name,
                "company_name": company_name,
                "created_at": datetime.utcnow().isoformat(),
                "is_active": True,
                "total_jobs": 0,
                "total_screenings": 0
            }
            
            self.users_container.create_item(body=user_data)
            return user_id
        
        except Exception as e:
            raise Exception(f"Failed to create user: {str(e)}")
    
    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Get user by email address
        
        Args:
            email: User email
        
        Returns:
            User data or None
        """
        try:
            query = "SELECT * FROM c WHERE c.email = @email"
            parameters = [{"name": "@email", "value": email.lower()}]
            
            items = list(self.users_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if items:
                return items[0]
            return None
        
        except Exception as e:
            raise Exception(f"Failed to retrieve user: {str(e)}")
    
    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get user by user ID
        
        Args:
            user_id: User ID
        
        Returns:
            User data or None
        """
        try:
            item = self.users_container.read_item(
                item=user_id,
                partition_key=user_id
            )
            return item
        
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            raise Exception(f"Failed to retrieve user: {str(e)}")
    
    async def update_user_stats(self, user_id: str, increment_jobs: int = 0, increment_screenings: int = 0):
        """
        Update user statistics
        
        Args:
            user_id: User ID
            increment_jobs: Number to increment total_jobs by
            increment_screenings: Number to increment total_screenings by
        """
        try:
            user_data = await self.get_user_by_id(user_id)
            if user_data:
                user_data["total_jobs"] = user_data.get("total_jobs", 0) + increment_jobs
                user_data["total_screenings"] = user_data.get("total_screenings", 0) + increment_screenings
                self.users_container.upsert_item(body=user_data)
        
        except Exception as e:
            print(f"Failed to update user stats: {str(e)}")
    
    # ==================== JOB DESCRIPTION MANAGEMENT ====================
    
    async def create_job_description(
        self,
        user_id: str,
        screening_name: str,
        job_description_text: str,
        must_have_skills: List[Dict],
        nice_to_have_skills: List[Dict],
        filename: Optional[str] = None,
        blob_url: Optional[str] = None
    ) -> str:
        """
        Create a new job description entry
        
        Args:
            user_id: ID of the user creating this job
            screening_name: Name/title for this screening
            job_description_text: Extracted or manual job description text
            must_have_skills: List of must-have skills
            nice_to_have_skills: List of nice-to-have skills
            filename: Optional original filename or "Manual Entry"
            blob_url: Optional Azure Blob URL for the job description file
        
        Returns:
            Job ID
        """
        try:
            job_id = str(uuid.uuid4())
            
            job_data = {
                "id": job_id,
                "job_id": job_id,
                "user_id": user_id,
                "screening_name": screening_name,
                "job_description_text": job_description_text,
                "filename": filename if filename else "Manual Entry",
                "must_have_skills": must_have_skills,
                "nice_to_have_skills": nice_to_have_skills,
                "created_at": datetime.utcnow().isoformat(),
                "total_screenings": 0,
                "total_candidates": 0,
                "status": "active"
            }
            
            if blob_url:
                job_data["blob_url"] = blob_url
            
            self.jobs_container.create_item(body=job_data)
            
            # Update user statistics
            await self.update_user_stats(user_id, increment_jobs=1)
            
            return job_id
        
        except Exception as e:
            raise Exception(f"Failed to create job description in database: {str(e)}")
    
    async def get_job_description(self, job_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job description by ID (user-specific)
        
        Args:
            job_id: Job ID
            user_id: User ID (partition key)
        
        Returns:
            Job description data or None
        """
        try:
            item = self.jobs_container.read_item(
                item=job_id,
                partition_key=user_id
            )
            return item
        
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            raise Exception(f"Failed to retrieve job description: {str(e)}")
    
    async def update_job_screening_count(self, job_id: str, user_id: str):
        """
        Increment the screening count for a job
        
        Args:
            job_id: Job ID
            user_id: User ID
        """
        try:
            job_data = await self.get_job_description(job_id, user_id)
            if job_data:
                job_data["total_screenings"] = job_data.get("total_screenings", 0) + 1
                job_data["total_candidates"] = job_data.get("total_candidates", 0) + 1
                job_data["last_screening_at"] = datetime.utcnow().isoformat()
                self.jobs_container.upsert_item(body=job_data)
                
                # Update user statistics
                await self.update_user_stats(user_id, increment_screenings=1)
        
        except Exception as e:
            print(f"Failed to update screening count: {str(e)}")
    
    async def save_screening_result(
        self,
        job_id: str,
        user_id: str,
        candidate_report: Dict[str, Any]
    ) -> str:
        """
        Save screening result for a candidate
        
        Args:
            job_id: Job ID
            user_id: User ID
            candidate_report: Candidate screening report
        
        Returns:
            Screening result ID
        """
        try:
            screening_id = str(uuid.uuid4())
            
            screening_data = {
                "id": screening_id,
                "job_id": job_id,
                "user_id": user_id,
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
            await self.update_job_screening_count(job_id, user_id)
            
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
    
    async def get_all_jobs_with_counts(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Get all job descriptions for a specific user with screening counts
        
        Args:
            user_id: User ID
        
        Returns:
            List of all jobs for the user with counts
        """
        try:
            query = "SELECT * FROM c WHERE c.user_id = @user_id ORDER BY c.created_at DESC"
            parameters = [{"name": "@user_id", "value": user_id}]
            
            items = list(self.jobs_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            
            # Enrich each job with screening counts
            for job in items:
                job_id = job.get("job_id")
                
                # Get count of screenings for this job
                screening_count_query = "SELECT VALUE COUNT(1) FROM c WHERE c.job_id = @job_id"
                screening_count_params = [{"name": "@job_id", "value": job_id}]
                
                try:
                    count_result = list(self.screenings_container.query_items(
                        query=screening_count_query,
                        parameters=screening_count_params,
                        enable_cross_partition_query=False,
                        partition_key=job_id
                    ))
                    
                    actual_count = count_result[0] if count_result else 0
                    
                    job["total_screenings"] = actual_count
                    job["total_candidates"] = actual_count
                    
                except Exception as e:
                    print(f"Error getting count for job {job_id}: {str(e)}")
                    job["total_screenings"] = job.get("total_screenings", 0)
                    job["total_candidates"] = job.get("total_candidates", 0)
            
            return items
        
        except Exception as e:
            raise Exception(f"Failed to retrieve all jobs with counts: {str(e)}")
        
    async def get_jobs_with_filters(
        self,
        user_id: str,
        search: Optional[str] = None,
        page_number: int = 1,
        page_size: int = 10,
        sort_by: str = "all"
    ) -> Dict[str, Any]:
        """
        Get jobs for a user with search, pagination, and date filtering
        
        Args:
            user_id: User ID
            search: Optional search term for screening_name
            page_number: Page number (1-indexed)
            page_size: Number of records per page
            sort_by: Date filter - "week", "month", or "all"
        
        Returns:
            Dictionary with paginated results and metadata
        """
        try:
            from datetime import datetime, timedelta
            
            # Build the query
            query_parts = ["SELECT * FROM c WHERE c.user_id = @user_id"]
            parameters = [{"name": "@user_id", "value": user_id}]
            
            # Add date filter
            if sort_by == "week":
                one_week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
                query_parts.append("AND c.created_at >= @date_filter")
                parameters.append({"name": "@date_filter", "value": one_week_ago})
            elif sort_by == "month":
                one_month_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()
                query_parts.append("AND c.created_at >= @date_filter")
                parameters.append({"name": "@date_filter", "value": one_month_ago})
            # "all" doesn't add any date filter
            
            # Add search filter
            if search and search.strip():
                query_parts.append("AND (CONTAINS(LOWER(c.screening_name), @search) OR CONTAINS(LOWER(c.job_description_text), @search))")
                parameters.append({"name": "@search", "value": search.lower().strip()})
            
            # Add sorting (most recent first)
            query_parts.append("ORDER BY c.created_at DESC")
            
            # Build complete query
            query = " ".join(query_parts)
            
            # Execute query to get all matching items
            all_items = list(self.jobs_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            
            # Calculate pagination
            total_jobs = len(all_items)
            total_pages = (total_jobs + page_size - 1) // page_size  # Ceiling division
            
            # Validate page_number
            if page_number < 1:
                page_number = 1
            if page_number > total_pages and total_pages > 0:
                page_number = total_pages
            
            # Calculate offset
            offset = (page_number - 1) * page_size
            
            # Get paginated items
            paginated_items = all_items[offset:offset + page_size]
            
            # Enrich each job with screening counts
            for job in paginated_items:
                job_id = job.get("job_id")
                
                # Get count of screenings for this job
                screening_count_query = "SELECT VALUE COUNT(1) FROM c WHERE c.job_id = @job_id"
                screening_count_params = [{"name": "@job_id", "value": job_id}]
                
                try:
                    count_result = list(self.screenings_container.query_items(
                        query=screening_count_query,
                        parameters=screening_count_params,
                        enable_cross_partition_query=False,
                        partition_key=job_id
                    ))
                    
                    actual_count = count_result[0] if count_result else 0
                    
                    job["total_screenings"] = actual_count
                    job["total_candidates"] = actual_count
                    
                except Exception as e:
                    print(f"Error getting count for job {job_id}: {str(e)}")
                    job["total_screenings"] = job.get("total_screenings", 0)
                    job["total_candidates"] = job.get("total_candidates", 0)
            
            return {
                "total_jobs": total_jobs,
                "total_pages": total_pages if total_pages > 0 else 1,
                "current_page": page_number,
                "page_size": page_size,
                "jobs": paginated_items
            }
        
        except Exception as e:
            raise Exception(f"Failed to retrieve jobs with filters: {str(e)}")
    
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
    
    async def delete_job_and_screenings(self, job_id: str, user_id: str) -> bool:
        """
        Delete job and all associated screening results
        
        Args:
            job_id: Job ID
            user_id: User ID
        
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
                partition_key=user_id
            )
            
            return True
        
        except Exception as e:
            print(f"Failed to delete job and screenings: {str(e)}")
            return False