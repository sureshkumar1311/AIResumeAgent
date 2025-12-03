"""
AI Resume Screener - FastAPI Backend with User Authentication
Handles job description upload and resume screening with detailed AI analysis
"""

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional, Dict
import json
from datetime import datetime

from models import (
    JobDescriptionResponse,
    ResumeScreeningResponse,
    CandidateReport,
    UserRegister,
    UserLogin,
    LoginResponse,
    UserResponse,
    JobListingResponse,
    JobListingRequest

)
from services.azure_blob_service import AzureBlobService
from services.document_parser import DocumentParser
from services.ai_screening_service import AIScreeningService
from services.cosmos_db_service import CosmosDBService
from services.auth_service import AuthService
from config import settings

app = FastAPI(
    title="AI Resume Screener API",
    description="Intelligent resume screening system with Azure OpenAI and User Authentication",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security scheme
security = HTTPBearer()

# Initialize services
blob_service = AzureBlobService()
document_parser = DocumentParser()
ai_service = AIScreeningService()
cosmos_service = CosmosDBService()
auth_service = AuthService()


# ==================== AUTHENTICATION DEPENDENCY ====================

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict:
    """
    Dependency to get current authenticated user from JWT token
    
    Args:
        credentials: Bearer token from Authorization header
    
    Returns:
        User data dictionary
    
    Raises:
        HTTPException: If token is invalid or user not found
    """
    token = credentials.credentials
    
    # Decode token
    payload = auth_service.decode_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token"
        )
    
    # Get user from database
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Invalid token payload"
        )
    
    user = await cosmos_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="User not found"
        )
    
    if not user.get("is_active", False):
        raise HTTPException(
            status_code=401,
            detail="User account is inactive"
        )
    
    return user


# ==================== PUBLIC ENDPOINTS ====================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "AI Resume Screener",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/api/auth/register", response_model=LoginResponse)
async def register_user(user_data: UserRegister):
    """
    Register a new user
    
    Args:
        user_data: User registration data (email, password, full_name, company_name)
    
    Returns:
        LoginResponse with access token and user info
    """
    try:
        # Check if user already exists
        existing_user = await cosmos_service.get_user_by_email(user_data.email)
        if existing_user:
            raise HTTPException(
                status_code=400,
                detail="User with this email already exists"
            )
        
        # Hash password
        hashed_password = auth_service.hash_password(user_data.password)
        
        # Create user
        user_id = await cosmos_service.create_user(
            email=user_data.email,
            hashed_password=hashed_password,
            full_name=user_data.full_name,
            company_name=user_data.company_name
        )
        
        # Get created user
        user = await cosmos_service.get_user_by_id(user_id)
        
        # Create access token
        access_token = auth_service.create_access_token(
            data={"user_id": user_id, "email": user_data.email}
        )
        
        # Prepare user response (remove sensitive data)
        user_response = UserResponse(
            user_id=user["user_id"],
            email=user["email"],
            full_name=user["full_name"],
            company_name=user.get("company_name"),
            created_at=user["created_at"],
            is_active=user["is_active"],
            total_jobs=user.get("total_jobs", 0),
            total_screenings=user.get("total_screenings", 0)
        )
        
        return LoginResponse(
            access_token=access_token,
            token_type="bearer",
            user=user_response
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auth/login", response_model=LoginResponse)
async def login_user(login_data: UserLogin):
    """
    Login user and get access token
    
    Args:
        login_data: User login credentials (email, password)
    
    Returns:
        LoginResponse with access token and user info
    """
    try:
        # Get user by email
        user = await cosmos_service.get_user_by_email(login_data.email)
        if not user:
            raise HTTPException(
                status_code=401,
                detail="Invalid email or password"
            )
        
        # Verify password
        if not auth_service.verify_password(login_data.password, user["hashed_password"]):
            raise HTTPException(
                status_code=401,
                detail="Invalid email or password"
            )
        
        # Check if user is active
        if not user.get("is_active", False):
            raise HTTPException(
                status_code=401,
                detail="User account is inactive"
            )
        
        # Create access token
        access_token = auth_service.create_access_token(
            data={"user_id": user["user_id"], "email": user["email"]}
        )
        
        # Prepare user response (remove sensitive data)
        user_response = UserResponse(
            user_id=user["user_id"],
            email=user["email"],
            full_name=user["full_name"],
            company_name=user.get("company_name"),
            created_at=user["created_at"],
            is_active=user["is_active"],
            total_jobs=user.get("total_jobs", 0),
            total_screenings=user.get("total_screenings", 0)
        )
        
        return LoginResponse(
            access_token=access_token,
            token_type="bearer",
            user=user_response
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== PROTECTED ENDPOINTS ====================

@app.get("/api/auth/me", response_model=UserResponse)
async def get_current_user_info(current_user: Dict = Depends(get_current_user)):
    """
    Get current authenticated user information
    
    Returns:
        Current user data
    """
    return UserResponse(
        user_id=current_user["user_id"],
        email=current_user["email"],
        full_name=current_user["full_name"],
        company_name=current_user.get("company_name"),
        created_at=current_user["created_at"],
        is_active=current_user["is_active"],
        total_jobs=current_user.get("total_jobs", 0),
        total_screenings=current_user.get("total_screenings", 0)
    )


@app.post("/api/job-description", response_model=JobDescriptionResponse)
async def upload_job_description(
    screening_name: str = Form(...),
    must_have_skills: str = Form(...),
    nice_to_have_skills: str = Form(...),
    job_description_file: UploadFile = File(None),
    description: str = Form(None),
    current_user: Dict = Depends(get_current_user)
):
    """
    Upload job description and skills (Protected - requires authentication)
    
    Args:
        screening_name: Name/title for this screening
        job_description_file: Optional PDF or Word document
        description: Optional manual text entry
        must_have_skills: JSON array of skill strings
        nice_to_have_skills: JSON array of skill strings
        current_user: Authenticated user (injected by dependency)
    
    Returns:
        JobDescriptionResponse with job_id
    """
    try:
        # Validate inputs
        if not job_description_file and not description:
            raise HTTPException(
                status_code=400,
                detail="Either job_description_file or description text must be provided."
            )
        
        if job_description_file and description:
            raise HTTPException(
                status_code=400,
                detail="Please provide either job_description_file OR description text, not both."
            )
        
        # Validate file type if provided
        if job_description_file and job_description_file.filename:
            if not job_description_file.filename.lower().endswith(('.pdf', '.docx', '.doc')):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid file format. Only PDF and Word documents are supported."
                )
        
        # Parse skills
        try:
            must_have_skills_list = json.loads(must_have_skills)
            nice_to_have_skills_list = json.loads(nice_to_have_skills)
            
            must_have_skills_objects = [
                {"skill": skill, "weight": 8} for skill in must_have_skills_list
            ]
            nice_to_have_skills_objects = [
                {"skill": skill, "weight": 5} for skill in nice_to_have_skills_list
            ]
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400,
                detail="Invalid JSON format for skills"
            )
        
        blob_url = None
        filename = None
        job_description_text = None
        
        # Process file upload if provided
        if job_description_file and job_description_file.filename:
            file_content = await job_description_file.read()
            blob_url = await blob_service.upload_file(
                file_content,
                f"job-descriptions/{current_user['user_id']}/{datetime.utcnow().timestamp()}_{job_description_file.filename}",
                content_type=job_description_file.content_type
            )
            filename = job_description_file.filename
            job_description_text = await document_parser.parse_document(
                file_content,
                job_description_file.filename
            )
        else:
            job_description_text = description
            filename = "Manual Entry"
        
        # Create job entry
        job_id = await cosmos_service.create_job_description(
            user_id=current_user["user_id"],
            screening_name=screening_name,
            job_description_text=job_description_text,
            must_have_skills=must_have_skills_objects,
            nice_to_have_skills=nice_to_have_skills_objects,
            filename=filename,
            blob_url=blob_url
        )
        
        return JobDescriptionResponse(
            job_id=job_id,
            message="Job description uploaded successfully",
            blob_url=blob_url if blob_url else "Manual entry - no file uploaded",
            must_have_skills_count=len(must_have_skills_list),
            nice_to_have_skills_count=len(nice_to_have_skills_list)
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/screen-resumes", response_model=ResumeScreeningResponse)
async def screen_resumes(
    job_id: str = Form(...),
    resumes: List[UploadFile] = File(...),
    current_user: Dict = Depends(get_current_user)
):
    """
    Screen resumes against job description (Protected - requires authentication)
    
    Args:
        job_id: Job ID
        resumes: List of resume files
        current_user: Authenticated user (injected by dependency)
    
    Returns:
        ResumeScreeningResponse with candidate reports
    """
    try:
        if not resumes:
            raise HTTPException(
                status_code=400,
                detail="At least one resume file is required"
            )
        
        if len(resumes) > settings.MAX_RESUMES_PER_BATCH:
            raise HTTPException(
                status_code=400,
                detail=f"Maximum {settings.MAX_RESUMES_PER_BATCH} resumes allowed per batch"
            )
        
        # Validate file types
        for resume in resumes:
            if not resume.filename.lower().endswith(('.pdf', '.docx', '.doc')):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid file format for {resume.filename}"
                )
        
        # Retrieve job description
        job_data = await cosmos_service.get_job_description(job_id, current_user["user_id"])
        if not job_data:
            raise HTTPException(
                status_code=404,
                detail="Job description not found or access denied"
            )
        
        candidate_reports = []
        
        # Process each resume
        for resume_file in resumes:
            try:
                resume_content = await resume_file.read()
                
                # Upload resume
                resume_blob_url = await blob_service.upload_file(
                    resume_content,
                    f"resumes/{job_id}/{datetime.utcnow().timestamp()}_{resume_file.filename}",
                    content_type=resume_file.content_type
                )
                
                # Parse resume
                resume_text = await document_parser.parse_document(
                    resume_content,
                    resume_file.filename
                )
                
                # Perform AI screening
                screening_result = await ai_service.screen_candidate(
                    resume_text=resume_text,
                    job_description=job_data["job_description_text"],
                    must_have_skills=job_data["must_have_skills"],
                    nice_to_have_skills=job_data["nice_to_have_skills"]
                )
                
                # Create candidate report
                candidate_report = CandidateReport(
                    candidate_name=screening_result["candidate_info"]["name"],
                    email=screening_result["candidate_info"].get("email"),
                    phone=screening_result["candidate_info"].get("phone"),
                    position=screening_result["candidate_info"]["position"],
                    location=screening_result["candidate_info"]["location"],
                    total_experience=screening_result["candidate_info"]["total_experience"],
                    resume_url=resume_blob_url,
                    resume_filename=resume_file.filename,
                    fit_score=screening_result["fit_score"],
                    must_have_skills_matched=screening_result["skills_analysis"]["must_have_matched"],
                    must_have_skills_total=screening_result["skills_analysis"]["must_have_total"],
                    nice_to_have_skills_matched=screening_result["skills_analysis"]["nice_to_have_matched"],
                    nice_to_have_skills_total=screening_result["skills_analysis"]["nice_to_have_total"],
                    matched_must_have_skills=screening_result["skills_analysis"]["matched_must_have_list"],
                    matched_nice_to_have_skills=screening_result["skills_analysis"]["matched_nice_to_have_list"],
                    ai_summary=screening_result["ai_summary"],
                    skill_depth_analysis=screening_result["skill_depth_analysis"],
                    professional_summary=screening_result["professional_summary"],
                    company_tier_analysis=screening_result["company_tier_analysis"]
                )
                
                # Save to database
                await cosmos_service.save_screening_result(
                    job_id=job_id,
                    user_id=current_user["user_id"],
                    candidate_report=candidate_report.dict()
                )
                
                candidate_reports.append(candidate_report)
                
            except Exception as e:
                print(f"Error processing resume {resume_file.filename}: {str(e)}")
                continue
        
        if not candidate_reports:
            raise HTTPException(
                status_code=500,
                detail="Failed to process any resumes"
            )
        
        return ResumeScreeningResponse(
            job_id=job_id,
            total_resumes_processed=len(candidate_reports),
            candidates=candidate_reports,
            processing_timestamp=datetime.utcnow().isoformat()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/jobs")
async def get_all_jobs(current_user: Dict = Depends(get_current_user)):
    """
    Get all job descriptions for current user (Protected)
    
    Returns:
        List of all jobs with screening counts
    """
    try:
        jobs = await cosmos_service.get_all_jobs_with_counts(current_user["user_id"])
        
        return {
            "total_jobs": len(jobs),
            "jobs": jobs
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/jobs/filter", response_model=JobListingResponse)
async def get_jobs_with_filters(
    filters: JobListingRequest,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get job descriptions for current user with advanced filters (Protected)
    
    Args:
        filters: JobListingRequest with search, pagination, and sorting
        current_user: Authenticated user
    
    Returns:
        JobListingResponse with paginated jobs and metadata
    
    Example Request:
    {
        "search": "python developer",
        "pageNumber": 1,
        "pageSize": 10,
        "sortBy": "week"
    }
    """
    try:
        result = await cosmos_service.get_jobs_with_filters(
            user_id=current_user["user_id"],
            search=filters.search,
            page_number=filters.pageNumber,
            page_size=filters.pageSize,
            sort_by=filters.sortBy
        )
        
        return JobListingResponse(
            total_jobs=result["total_jobs"],
            total_pages=result["total_pages"],
            current_page=result["current_page"],
            page_size=result["page_size"],
            jobs=result["jobs"]
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/job/{job_id}")
async def get_job_details(
    job_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get job details with all screening results (Protected)
    
    Args:
        job_id: Job ID
        current_user: Authenticated user
    
    Returns:
        Complete job details with screening results
    """
    try:
        # Get job details
        job_data = await cosmos_service.get_job_description(job_id, current_user["user_id"])
        if not job_data:
            raise HTTPException(
                status_code=404,
                detail=f"Job not found or access denied"
            )
        
        # Get screening results
        screening_results = await cosmos_service.get_screening_results(job_id)
        
        job_data["screening_results"] = screening_results
        job_data["total_candidates_screened"] = len(screening_results)
        
        return job_data
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/candidate/{candidate_id}")
async def get_candidate_report(
    candidate_id: str,
    job_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get candidate screening report (Protected)
    
    Args:
        candidate_id: Candidate/Screening ID
        job_id: Job ID (query parameter)
        current_user: Authenticated user
    
    Returns:
        Complete candidate screening report
    """
    try:
        # Verify job belongs to user
        job_data = await cosmos_service.get_job_description(job_id, current_user["user_id"])
        if not job_data:
            raise HTTPException(
                status_code=404,
                detail="Job not found or access denied"
            )
        
        result = await cosmos_service.get_screening_by_id(candidate_id, job_id)
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Candidate report not found"
            )
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run('main:app', host="0.0.0.0", port=8000, reload=True)
