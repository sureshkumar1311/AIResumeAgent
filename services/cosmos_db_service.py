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
            # REMOVED offer_throughput for serverless compatibility
            self.jobs_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_JOBS,
                partition_key=PartitionKey(path="/user_id")
            )
            
            # Create screenings container if not exists
            self.screenings_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_SCREENINGS,
                partition_key=PartitionKey(path="/job_id")
            )
            
            # Create users container if not exists
            self.users_container = self.database.create_container_if_not_exists(
                id=settings.COSMOS_DB_CONTAINER_USERS,
                partition_key=PartitionKey(path="/user_id")
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
        must_have_skills: List[str],  # Changed from List[Dict] to List[str]
        nice_to_have_skills: List[str],  # Changed from List[Dict] to List[str]
        filename: Optional[str] = None,
        blob_url: Optional[str] = None
    ) -> str:
        """
        Create a new job description entry
        
        Args:
            user_id: ID of the user creating this job
            screening_name: Name/title for this screening
            job_description_text: Extracted or manual job description text
            must_have_skills: List of must-have skill strings (auto-extracted)
            nice_to_have_skills: List of nice-to-have skill strings (auto-extracted)
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
                "must_have_skills": must_have_skills,  # Now just list of strings
                "nice_to_have_skills": nice_to_have_skills,  # Now just list of strings
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

    async def create_screening_job(
        self,
        screening_job_id: str,
        job_id: str,
        user_id: str,
        total_resumes: int
    ) -> str:
        """
        Create a screening job entry to track batch progress
        
        Args:
            screening_job_id: Unique screening job ID
            job_id: Job description ID
            user_id: User ID
            total_resumes: Total number of resumes to process
        
        Returns:
            Screening job ID
        """
        try:
            # Create screening_jobs container if not exists
            if not hasattr(self, 'screening_jobs_container'):
                self.screening_jobs_container = self.database.create_container_if_not_exists(
                    id=settings.COSMOS_DB_CONTAINER_SCREENING_JOBS,
                    partition_key=PartitionKey(path="/user_id"),
                    offer_throughput=400
                )
            
            screening_job_data = {
                "id": screening_job_id,
                "screening_job_id": screening_job_id,
                "job_id": job_id,
                "user_id": user_id,
                "total_resumes": total_resumes,
                "processed_resumes": 0,
                "successful_resumes": 0,
                "failed_resumes": 0,
                "status": "processing",  # processing, completed, failed
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "resume_statuses": []  # List of {filename, status, processed_at}
            }
            
            self.screening_jobs_container.create_item(body=screening_job_data)
            return screening_job_id
        
        except Exception as e:
            raise Exception(f"Failed to create screening job: {str(e)}")
        
    async def get_screening_job(self, screening_job_id: str) -> Optional[Dict[str, Any]]:
        """Get screening job by ID"""
        try:
            if not hasattr(self, 'screening_jobs_container'):
                return None
            
            query = "SELECT * FROM c WHERE c.screening_job_id = @screening_job_id"
            parameters = [{"name": "@screening_job_id", "value": screening_job_id}]
            
            items = list(self.screening_jobs_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            return items[0] if items else None
        
        except Exception as e:
            print(f"Error getting screening job: {str(e)}")
            return None
        
    async def get_screening_job_status(
        self,
        screening_job_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get current status of a screening job (for polling)
        
        Args:
            screening_job_id: Screening job ID
            user_id: User ID (for authorization)
        
        Returns:
            Status dictionary with progress information
        """
        try:
            screening_job = await self.get_screening_job(screening_job_id)
            
            if not screening_job or screening_job.get("user_id") != user_id:
                return None
            
            # Get completed screening results
            completed_screenings = await self.get_screening_results(
                job_id=screening_job["job_id"]
            )
            
            # Filter only screenings from this batch (if needed)
            # For now, return all recent ones
            
            return {
                "screening_job_id": screening_job_id,
                "status": screening_job["status"],
                "total_resumes": screening_job["total_resumes"],
                "processed_resumes": screening_job["processed_resumes"],
                "successful_resumes": screening_job["successful_resumes"],
                "failed_resumes": screening_job["failed_resumes"],
                "progress_percentage": screening_job.get("progress_percentage", 0),
                "created_at": screening_job["created_at"],
                "updated_at": screening_job["updated_at"],
                "completed_results": completed_screenings[-screening_job["processed_resumes"]:] if completed_screenings else []
            }
        
        except Exception as e:
            print(f"Error getting screening job status: {str(e)}")
            return None
        
    async def update_screening_job_progress(
        self,
        screening_job_id: str,
        resume_filename: str,
        status: str,  # "success" or "failed"
        screening_id: Optional[str] = None
    ) -> bool:
        """
        Update progress of a screening job after processing one resume
        
        Args:
            screening_job_id: Screening job ID
            resume_filename: Name of the processed resume
            status: "success" or "failed"
            screening_id: Optional screening result ID
        
        Returns:
            True if updated successfully
        """
        try:
            if not hasattr(self, 'screening_jobs_container'):
                return False
            
            # Get current screening job
            screening_job = await self.get_screening_job(screening_job_id)
            if not screening_job:
                return False
            
            # Update counters
            screening_job["processed_resumes"] += 1
            
            if status == "success":
                screening_job["successful_resumes"] += 1
            else:
                screening_job["failed_resumes"] += 1
            
            # Add resume status
            screening_job["resume_statuses"].append({
                "filename": resume_filename,
                "status": status,
                "processed_at": datetime.utcnow().isoformat(),
                "screening_id": screening_id
            })
            
            # Update overall status
            if screening_job["processed_resumes"] >= screening_job["total_resumes"]:
                screening_job["status"] = "completed"
            
            screening_job["updated_at"] = datetime.utcnow().isoformat()
            
            # Calculate progress percentage
            screening_job["progress_percentage"] = int(
                (screening_job["processed_resumes"] / screening_job["total_resumes"]) * 100
            )
            
            # Update in database
            self.screening_jobs_container.upsert_item(body=screening_job)
            
            print(f" Progress: {screening_job['processed_resumes']}/{screening_job['total_resumes']} ({screening_job['progress_percentage']}%)")
            
            return True
        
        except Exception as e:
            print(f"Error updating screening job progress: {str(e)}")
            return False
    
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
        
    # Add this method to the CosmosDBService class

    async def get_jobs_with_filters(
        self,
        user_id: str,
        search: Optional[str] = None,
        page_number: int = 1,
        page_size: int = 10,
        sort_by: str = "recent"
    ) -> Dict[str, Any]:
        """
        Get jobs for a user with advanced filtering, pagination, and sorting
        
        Args:
            user_id: User ID
            search: Search term for screening_name or job_description_text
            page_number: Page number (starts from 1)
            page_size: Number of items per page
            sort_by: Sort order - 'recent', 'oldest', 'week', 'month', 'name'
        
        Returns:
            Dictionary with jobs, pagination metadata
        """
        try:
            from datetime import datetime, timedelta
            
            # Build query conditions
            conditions = ["c.user_id = @user_id"]
            parameters = [{"name": "@user_id", "value": user_id}]
            
            # Add search filter
            if search:
                conditions.append("(CONTAINS(LOWER(c.screening_name), LOWER(@search)) OR CONTAINS(LOWER(c.job_description_text), LOWER(@search)))")
                parameters.append({"name": "@search", "value": search})
            
            # Add date filters for 'week' or 'month'
            if sort_by == "week":
                week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
                conditions.append("c.created_at >= @date_filter")
                parameters.append({"name": "@date_filter", "value": week_ago})
            elif sort_by == "month":
                month_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()
                conditions.append("c.created_at >= @date_filter")
                parameters.append({"name": "@date_filter", "value": month_ago})
            
            # Build WHERE clause
            where_clause = " AND ".join(conditions)
            
            # Determine ORDER BY clause
            if sort_by == "oldest":
                order_by = "ORDER BY c.created_at ASC"
            elif sort_by == "name":
                order_by = "ORDER BY c.screening_name ASC"
            else:  # 'recent', 'week', 'month' all sort by recent
                order_by = "ORDER BY c.created_at DESC"
            
            # Count total matching jobs
            count_query = f"SELECT VALUE COUNT(1) FROM c WHERE {where_clause}"
            count_result = list(self.jobs_container.query_items(
                query=count_query,
                parameters=parameters,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            total_jobs = count_result[0] if count_result else 0
            
            # Calculate pagination
            total_pages = (total_jobs + page_size - 1) // page_size  # Ceiling division
            offset = (page_number - 1) * page_size
            
            # Get paginated jobs
            query = f"""
            SELECT * FROM c 
            WHERE {where_clause}
            {order_by}
            OFFSET {offset} LIMIT {page_size}
            """
            
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
            
            return {
                "total_jobs": total_jobs,
                "total_pages": total_pages,
                "current_page": page_number,
                "page_size": page_size,
                "jobs": items
            }
        
        except Exception as e:
            raise Exception(f"Failed to get jobs with filters: {str(e)}")
    
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
        
    # Add this method to the CosmosDBService class

    async def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a user
        
        Args:
            user_id: User ID
        
        Returns:
            Dictionary with user statistics
        """
        try:
            # Get all jobs for user
            jobs_query = "SELECT * FROM c WHERE c.user_id = @user_id"
            jobs_params = [{"name": "@user_id", "value": user_id}]
            
            jobs = list(self.jobs_container.query_items(
                query=jobs_query,
                parameters=jobs_params,
                enable_cross_partition_query=False,
                partition_key=user_id
            ))
            
            total_job_descriptions = len(jobs)
            total_resumes_screened = 0
            jobs_with_screenings = 0
            jobs_summary = []
            
            # Get screening counts for each job
            for job in jobs:
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
                    
                    screening_count = count_result[0] if count_result else 0
                    total_resumes_screened += screening_count
                    
                    if screening_count > 0:
                        jobs_with_screenings += 1
                    
                    jobs_summary.append({
                        "job_id": job_id,
                        "screening_name": job.get("screening_name"),
                        "created_at": job.get("created_at"),
                        "total_screenings": screening_count,
                        "must_have_skills": job.get("must_have_skills", []),
                        "nice_to_have_skills": job.get("nice_to_have_skills", [])
                    })
                    
                except Exception as e:
                    print(f"Error getting screenings for job {job_id}: {str(e)}")
                    jobs_summary.append({
                        "job_id": job_id,
                        "screening_name": job.get("screening_name"),
                        "created_at": job.get("created_at"),
                        "total_screenings": 0,
                        "must_have_skills": job.get("must_have_skills", []),
                        "nice_to_have_skills": job.get("nice_to_have_skills", [])
                    })
            
            return {
                "user_id": user_id,
                "total_job_descriptions": total_job_descriptions,
                "total_resumes_screened": total_resumes_screened,
                "total_jobs_with_screenings": jobs_with_screenings,
                "jobs_summary": jobs_summary
            }
        
        except Exception as e:
            raise Exception(f"Failed to get user statistics: {str(e)}")
        
    # Add these methods to the existing CosmosDBService class

    async def get_screening_job_by_job_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get screening job by job_id
        
        Args:
            job_id: Job description ID
        
        Returns:
            Screening job data or None
        """
        try:
            # Initialize container if needed
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    # Container doesn't exist yet
                    return None
            
            # Try to read item directly using job_id as both id and partition key
            try:
                item = self.screening_jobs_container.read_item(
                    item=job_id,
                    partition_key=job_id
                )
                return item
            except exceptions.CosmosResourceNotFoundError:
                # Item doesn't exist
                return None
        
        except Exception as e:
            print(f"Error getting screening job: {str(e)}")
            return None


    async def initialize_screening_job_for_job(
        self,
        job_id: str,
        user_id: str
    ) -> bool:
        """
        Initialize screening job tracking for a job_id
        FIXED: Ensure container exists and handle conflicts properly
        """
        try:
            # Ensure container exists
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    # Try to get existing container first
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    # Container doesn't exist, create it
                    self.screening_jobs_container = self.database.create_container_if_not_exists(
                        id=settings.COSMOS_DB_CONTAINER_SCREENING_JOBS,
                        partition_key=PartitionKey(path="/job_id")
                    )
            
            # Check if screening job already exists
            existing = await self.get_screening_job_by_job_id(job_id)
            if existing:
                print(f"        Screening job already exists for job_id: {job_id}")
                return True
            
            # Create new screening job
            screening_job_data = {
                "id": job_id,
                "job_id": job_id,
                "user_id": user_id,
                "total_resumes": 0,  # Will be calculated from blob
                "processed_resumes": 0,
                "successful_resumes": 0,
                "failed_resumes": 0,
                "status": "processing",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "resume_statuses": []
            }
            
            try:
                self.screening_jobs_container.create_item(body=screening_job_data)
                print(f"       Created screening job tracker for job_id: {job_id}")
                return True
            except exceptions.CosmosResourceExistsError:
                # Another worker already created it (race condition)
                print(f"        Screening job created by another worker")
                return True
        
        except Exception as e:
            print(f" Error initializing screening job: {str(e)}")
            import traceback
            traceback.print_exc()
            return True  # Don't fail the whole process


    async def update_screening_job_progress_by_job_id(
        self,
        job_id: str,
        resume_filename: str,
        status: str,
        screening_id: Optional[str] = None
    ) -> bool:
        """
        Update progress of a screening job after processing one resume
         FIXED: Ensure container exists and handle properly
        """
        try:
            # Ensure container exists
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    print(f" Screening jobs container doesn't exist")
                    return False
            
            # Get current screening job
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if not screening_job:
                print(f"  Screening job not found, creating it now")
                # Try to get user_id from job
                query = "SELECT c.user_id FROM c WHERE c.job_id = @job_id"
                parameters = [{"name": "@job_id", "value": job_id}]
                jobs = list(self.jobs_container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True
                ))
                
                if jobs:
                    user_id = jobs[0].get("user_id")
                    await self.initialize_screening_job_for_job(job_id, user_id)
                    screening_job = await self.get_screening_job_by_job_id(job_id)
                
                if not screening_job:
                    print(f" Could not create/find screening job")
                    return False
            
            # Update counters
            screening_job["processed_resumes"] = screening_job.get("processed_resumes", 0) + 1
            
            if status == "success":
                screening_job["successful_resumes"] = screening_job.get("successful_resumes", 0) + 1
            else:
                screening_job["failed_resumes"] = screening_job.get("failed_resumes", 0) + 1
            
            # Add resume status
            if "resume_statuses" not in screening_job:
                screening_job["resume_statuses"] = []
            
            screening_job["resume_statuses"].append({
                "filename": resume_filename,
                "status": status,
                "processed_at": datetime.utcnow().isoformat(),
                "screening_id": screening_id
            })
            
            # Update timestamp
            screening_job["updated_at"] = datetime.utcnow().isoformat()
            
            # Update status (but don't set to completed - let the status endpoint calculate)
            screening_job["status"] = "processing"
            
            # Save to database
            self.screening_jobs_container.upsert_item(body=screening_job)
            
            print(f" Updated progress: Processed={screening_job['processed_resumes']}, Success={screening_job['successful_resumes']}, Failed={screening_job['failed_resumes']}")
            
            return True
        
        except Exception as e:
            print(f" Error updating screening job progress: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    # Add this method to CosmosDBService class

    async def is_resume_already_processed(self, job_id: str, resume_filename: str) -> bool:
        """
        Check if resume has already been processed (prevents duplicates)
        
        Args:
            job_id: Job ID
            resume_filename: Resume filename
        
        Returns:
            True if already processed, False otherwise
        """
        try:
            if not hasattr(self, 'screenings_container'):
                return False
            
            # Query to check if this resume was already processed
            query = "SELECT VALUE COUNT(1) FROM c WHERE c.job_id = @job_id AND c.resume_filename = @filename"
            parameters = [
                {"name": "@job_id", "value": job_id},
                {"name": "@filename", "value": resume_filename}
            ]
            
            result = list(self.screenings_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=job_id
            ))
            
            count = result[0] if result else 0
            return count > 0
        
        except Exception as e:
            print(f"Error checking duplicate: {str(e)}")
            return False


    async def get_total_resumes_in_blob(self, job_id: str) -> int:
        """
        Count total resumes uploaded in blob storage for a job
         FIXED: Proper blob listing and counting
        
        Args:
            job_id: Job ID
        
        Returns:
            Total count of resume files in blob storage
        """
        try:
            from azure.storage.blob import BlobServiceClient
            
            print(f"    Counting blobs for job_id: {job_id}")
            
            # Initialize blob client
            blob_service_client = BlobServiceClient.from_connection_string(
                settings.AZURE_STORAGE_CONNECTION_STRING
            )
            
            container_client = blob_service_client.get_container_client(
                settings.AZURE_STORAGE_CONTAINER_RESUMES
            )
            
            # List all blobs with job_id prefix
            blob_prefix = f"{job_id}/"
            print(f"    Looking for blobs with prefix: {blob_prefix}")
            print(f"    Container: {settings.AZURE_STORAGE_CONTAINER_RESUMES}")
            
            # List blobs
            blob_list = container_client.list_blobs(name_starts_with=blob_prefix)
            
            # Count blobs (excluding folders)
            count = 0
            blob_names = []
            
            for blob in blob_list:
                # Skip if it's a folder (ends with /)
                if not blob.name.endswith('/'):
                    count += 1
                    blob_names.append(blob.name)
                    print(f"      ✓ Found: {blob.name}")
            
            print(f"    Total blobs found: {count}")
            
            return count
        
        except Exception as e:
            print(f"    Error counting blobs: {str(e)}")
            import traceback
            traceback.print_exc()
            return 0


    async def get_screening_job_status_by_job_id(
        self,
        job_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get current status of a screening job by job_id (for polling)
         UPDATED: Shows total_resumes immediately from blob storage
        """
        try:
            # Verify job belongs to user
            job_data = await self.get_job_description(job_id, user_id)
            if not job_data:
                return None
            
            #  Get total resumes from BLOB STORAGE (not from tracker)
            total_resumes_in_blob = await self.get_total_resumes_in_blob(job_id)
            
            # Get screening job tracker
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if not screening_job:
                # No processing started yet, but show total from blob
                if total_resumes_in_blob > 0:
                    # Files uploaded but not processed yet
                    return {
                        "job_id": job_id,
                        "status": "pending",  # New status
                        "total_resumes": total_resumes_in_blob,
                        "processed_resumes": 0,
                        "successful_resumes": 0,
                        "failed_resumes": 0,
                        "progress_percentage": 0,
                        "completed_results": []
                    }
                else:
                    # No files uploaded
                    return {
                        "job_id": job_id,
                        "status": "no_resumes",
                        "total_resumes": 0,
                        "processed_resumes": 0,
                        "successful_resumes": 0,
                        "failed_resumes": 0,
                        "progress_percentage": 0,
                        "completed_results": []
                    }
            
            # Get completed screening results
            completed_screenings = await self.get_screening_results(job_id)
            
            #  Use blob count as source of truth for total
            processed_resumes = screening_job["processed_resumes"]
            
            # Calculate progress
            if total_resumes_in_blob > 0:
                progress_percentage = int((processed_resumes / total_resumes_in_blob) * 100)
            else:
                progress_percentage = 0
            
            # Determine status
            if processed_resumes >= total_resumes_in_blob and total_resumes_in_blob > 0:
                status = "completed"
            elif processed_resumes > 0:
                status = "processing"
            else:
                status = "pending"
            
            return {
                "job_id": job_id,
                "status": status,
                "total_resumes": total_resumes_in_blob,  #  From blob storage
                "processed_resumes": processed_resumes,
                "successful_resumes": screening_job.get("successful_resumes", 0),
                "failed_resumes": screening_job.get("failed_resumes", 0),
                "progress_percentage": progress_percentage,
                "created_at": screening_job.get("created_at"),
                "updated_at": screening_job.get("updated_at"),
                "completed_results": completed_screenings
            }
        
        except Exception as e:
            print(f"Error getting screening job status: {str(e)}")
            return None
        
    async def get_comprehensive_screening_status(
        self,
        job_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive screening status
         UPDATED: Better progress calculation with max 100%
        """
        try:
            # 1. Get full job details
            job_data = await self.get_job_description(job_id, user_id)
            if not job_data:
                print(f" Job not found: {job_id} for user: {user_id}")
                return None
            
            print(f" Found job: {job_data.get('screening_name')}")
            
            # 2. Get ALL screening results (all time)
            all_screenings = await self.get_screening_results(job_id)
            total_candidates_screened = len(all_screenings)
            print(f" Total candidates screened (all time): {total_candidates_screened}")
            
            # 3. Get screening job tracker
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            # 4. Calculate current batch progress
            if screening_job:
                print(f" Found screening job tracker:")
                
                total_in_batch = screening_job.get("total_resumes", 0)
                processed = screening_job.get("processed_resumes", 0)
                successful = screening_job.get("successful_resumes", 0)
                failed = screening_job.get("failed_resumes", 0)
                
                print(f"   - Total in current batch: {total_in_batch}")
                print(f"   - Processed: {processed}")
                print(f"   - Successful: {successful}")
                print(f"   - Failed: {failed}")
                
                #  FIX: Cap processed at total (in case of race conditions)
                processed = min(processed, total_in_batch)
                successful = min(successful, total_in_batch)
                
                remaining = max(0, total_in_batch - processed)
                
                #  FIX: Calculate progress with max 100%
                if total_in_batch > 0:
                    progress_percentage = min(100, int((processed / total_in_batch) * 100))
                else:
                    progress_percentage = 0
                
                # Determine status
                if total_in_batch == 0:
                    status = "no_resumes_in_queue"
                elif remaining == 0 and processed > 0:
                    status = "completed"
                    progress_percentage = 100  # Ensure it's exactly 100
                    print(f" Status: COMPLETED")
                elif processed > 0:
                    status = "processing"
                    print(f" Status: PROCESSING ({processed}/{total_in_batch})")
                else:
                    status = "pending"
                    print(f" Status: PENDING")
                
                current_batch = {
                    "total_uploaded_in_queue": total_in_batch,
                    "processed": processed,
                    "successful": successful,
                    "failed": failed,
                    "remaining": remaining,
                    "status": status,
                    "progress_percentage": progress_percentage,
                    "batch_start_time": screening_job.get("batch_start_time")
                }
            else:
                # No tracker = no active batch
                print(f"  No screening job tracker found")
                print(f" Status: NO_RESUMES_IN_QUEUE")
                current_batch = {
                    "total_uploaded_in_queue": 0,
                    "processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "remaining": 0,
                    "status": "no_resumes_in_queue",
                    "progress_percentage": 0,
                    "batch_start_time": None
                }
            
            # 5. Return complete response
            return {
                "job_id": job_data.get("job_id"),
                "screening_name": job_data.get("screening_name"),
                "job_description_text": job_data.get("job_description_text"),
                "filename": job_data.get("filename"),
                "must_have_skills": job_data.get("must_have_skills", []),
                "nice_to_have_skills": job_data.get("nice_to_have_skills", []),
                "created_at": job_data.get("created_at"),
                "blob_url": job_data.get("blob_url"),
                "total_candidates_screened": total_candidates_screened,
                "current_batch": current_batch,
                "screening_results": all_screenings
            }
        
        except Exception as e:
            print(f" Error in get_comprehensive_screening_status: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    async def initialize_or_increment_batch_total(
        self,
        job_id: str,
        user_id: str
    ) -> bool:
        """
        Initialize screening job tracker OR increment total_resumes count
         UPDATED: Resets tracker when new batch starts
        """
        try:
            # Ensure container exists
            if not hasattr(self, 'screening_jobs_container'):
                try:
                    self.screening_jobs_container = self.database.get_container_client(
                        settings.COSMOS_DB_CONTAINER_SCREENING_JOBS
                    )
                except:
                    self.screening_jobs_container = self.database.create_container_if_not_exists(
                        id=settings.COSMOS_DB_CONTAINER_SCREENING_JOBS,
                        partition_key=PartitionKey(path="/job_id")
                    )
            
            # Check if we should reset for new batch
            should_reset = await self.should_reset_tracker_for_new_batch(job_id)
            
            if should_reset:
                # Delete old tracker and create fresh one
                try:
                    self.screening_jobs_container.delete_item(
                        item=job_id,
                        partition_key=job_id
                    )
                    print(f"       Deleted old completed tracker")
                except:
                    pass
            
            # Try to get existing tracker
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if not screening_job:
                # Create new tracker with count = 1
                screening_job_data = {
                    "id": job_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "total_resumes": 1,  # First resume in this batch
                    "processed_resumes": 0,
                    "successful_resumes": 0,
                    "failed_resumes": 0,
                    "status": "processing",
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                    "batch_start_time": datetime.utcnow().isoformat(),  # Track batch start
                    "resume_statuses": []
                }
                
                try:
                    self.screening_jobs_container.create_item(body=screening_job_data)
                    print(f"       Created NEW screening job tracker: total_resumes=1")
                    return True
                except exceptions.CosmosResourceExistsError:
                    # Race condition - another message created it
                    screening_job = await self.get_screening_job_by_job_id(job_id)
                    if screening_job:
                        screening_job["total_resumes"] = screening_job.get("total_resumes", 0) + 1
                        screening_job["updated_at"] = datetime.utcnow().isoformat()
                        self.screening_jobs_container.upsert_item(body=screening_job)
                        print(f"       Incremented total_resumes to {screening_job['total_resumes']}")
                    return True
            else:
                # Tracker exists - increment total
                screening_job["total_resumes"] = screening_job.get("total_resumes", 0) + 1
                screening_job["updated_at"] = datetime.utcnow().isoformat()
                self.screening_jobs_container.upsert_item(body=screening_job)
                print(f"       Incremented total_resumes to {screening_job['total_resumes']}")
                return True
        
        except Exception as e:
            print(f" Error in initialize_or_increment_batch_total: {str(e)}")
            import traceback
            traceback.print_exc()
            return True 
        
    async def reset_screening_job_for_new_batch(
        self,
        job_id: str
    ) -> bool:
        """
        Reset screening job tracker for a new batch
        Call this when starting a completely new upload session
        
         OPTIONAL: Only needed if you want to clear tracker between batches
        """
        try:
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if screening_job:
                # Check if previous batch was completed
                if screening_job.get("status") == "completed":
                    # Delete old tracker to start fresh
                    self.screening_jobs_container.delete_item(
                        item=job_id,
                        partition_key=job_id
                    )
                    print(f" Deleted old completed tracker for new batch")
                    return True
            
            return False
        except Exception as e:
            print(f"Error resetting tracker: {str(e)}")
            return False

    async def should_reset_tracker_for_new_batch(
        self,
        job_id: str
    ) -> bool:
        """
        Check if we should reset the tracker for a new batch
        
        Returns True if:
        - Previous batch was completed (all files processed)
        - Some time has passed since last upload
        
        Returns:
            True if tracker should be reset, False otherwise
        """
        try:
            screening_job = await self.get_screening_job_by_job_id(job_id)
            
            if not screening_job:
                # No tracker exists = first batch
                return False
            
            total = screening_job.get("total_resumes", 0)
            processed = screening_job.get("processed_resumes", 0)
            
            # If previous batch was completed (all files processed)
            if total > 0 and processed >= total:
                print(f"        Previous batch was completed ({processed}/{total})")
                print(f"       Starting new batch - resetting tracker")
                return True
            
            return False
        
        except Exception as e:
            print(f"Error checking batch reset: {str(e)}")
            return False