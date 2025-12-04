"""
AI Resume Screener - FastAPI Backend with User Authentication
Handles job description upload and resume screening with detailed AI analysis
"""

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional, Dict
import json
import base64
from datetime import datetime

from models import (
    JobDescriptionRequest,
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
    job_data: JobDescriptionRequest,
    current_user: Dict = Depends(get_current_user)
):
    """
    Upload job description with JSON body (supports base64 file or text)
    
    Args:
        job_data: Job description request with screening_name, job_description_file (base64), 
                  or description text
        current_user: Authenticated user (injected by dependency)
    
    Returns:
        JobDescriptionResponse with job_id and auto-extracted skills
    
    Example JSON Body with Base64 File:
    {
        "screening_name": "Senior Python Developer - Q4 2024",
        "job_description_file": "data:application/pdf;base64,JVBERi0xLjQKJeLjz9MKMyAwIG9iago8PC9UeXBlIC9QYWdlCi9QYXJlbn..."
    }
    
    OR with just base64:
    {
        "screening_name": "Senior Python Developer - Q4 2024",
        "job_description_file": "JVBERi0xLjQKJeLjz9MKMyAwIG9iago8PC9UeXBlIC9QYWdlCi9QYXJlbn..."
    }
    
    OR with manual text:
    {
        "screening_name": "Senior Python Developer - Q4 2024",
        "description": "We are looking for a senior Python developer..."
    }
    """
    try:
        # Validate inputs
        if not job_data.job_description_file and not job_data.description:
            raise HTTPException(
                status_code=400,
                detail="Either job_description_file (base64) or description text must be provided."
            )
        
        if job_data.job_description_file and job_data.description:
            raise HTTPException(
                status_code=400,
                detail="Please provide either job_description_file OR description text, not both."
            )
        
        blob_url = None
        filename = None
        job_description_text = None
        
        # Process base64 file if provided
        if job_data.job_description_file:
            try:
                # Extract base64 data and determine file type
                base64_data = job_data.job_description_file
                file_extension = None
                content_type = None
                
                # Check if it's a data URI (data:mime/type;base64,xxxxx)
                if base64_data.startswith('data:'):
                    # Extract MIME type and base64 data
                    try:
                        header, encoded = base64_data.split(',', 1)
                        mime_type = header.split(':')[1].split(';')[0]
                        base64_data = encoded
                        
                        # Determine file extension from MIME type
                        if 'pdf' in mime_type.lower():
                            file_extension = '.pdf'
                            content_type = 'application/pdf'
                        elif 'word' in mime_type.lower() or 'document' in mime_type.lower():
                            file_extension = '.docx'
                            content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                        elif 'msword' in mime_type.lower():
                            file_extension = '.doc'
                            content_type = 'application/msword'
                        else:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Unsupported MIME type: {mime_type}. Only PDF and Word documents are supported."
                            )
                    except ValueError:
                        raise HTTPException(
                            status_code=400,
                            detail="Invalid data URI format. Expected format: data:mime/type;base64,xxxxx"
                        )
                else:
                    # No data URI prefix - try to detect file type from base64 content
                    # Decode first few bytes to detect file signature
                    try:
                        decoded_preview = base64.b64decode(base64_data[:100])
                        
                        # PDF signature: %PDF
                        if decoded_preview.startswith(b'%PDF'):
                            file_extension = '.pdf'
                            content_type = 'application/pdf'
                        # DOCX signature: PK (ZIP format)
                        elif decoded_preview.startswith(b'PK\x03\x04'):
                            file_extension = '.docx'
                            content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                        # DOC signature: D0CF11E0 (OLE format)
                        elif decoded_preview.startswith(b'\xD0\xCF\x11\xE0'):
                            file_extension = '.doc'
                            content_type = 'application/msword'
                        else:
                            raise HTTPException(
                                status_code=400,
                                detail="Unable to detect file type. Please provide base64 with data URI prefix (data:application/pdf;base64,...) or ensure file is PDF/DOCX format."
                            )
                    except Exception as e:
                        raise HTTPException(
                            status_code=400,
                            detail="Unable to detect file type from base64 content. Please use data URI format."
                        )
                
                # Generate filename with timestamp
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"job_description_{timestamp}{file_extension}"
                
                # Decode base64 to bytes
                file_content = base64.b64decode(base64_data)
                
                # Validate file size (optional)
                file_size_mb = len(file_content) / (1024 * 1024)
                if file_size_mb > settings.MAX_FILE_SIZE_MB:
                    raise HTTPException(
                        status_code=400,
                        detail=f"File size ({file_size_mb:.2f}MB) exceeds maximum allowed size ({settings.MAX_FILE_SIZE_MB}MB)"
                    )
                
                # Upload to blob storage
                blob_url = await blob_service.upload_file(
                    file_content,
                    f"job-descriptions/{current_user['user_id']}/{timestamp}_{filename}",
                    content_type=content_type
                )
                
                print(f"File uploaded: {filename} ({file_size_mb:.2f}MB)")
                
                # Parse document to extract text
                job_description_text = await document_parser.parse_document(
                    file_content,
                    filename
                )
                
            except base64.binascii.Error:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid base64 encoding for job_description_file"
                )
            except HTTPException:
                raise
            except Exception as e:
                print(f"Error processing file: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to process file: {str(e)}"
                )
        else:
            # Use manual description text
            job_description_text = job_data.description
            filename = "Manual Entry"
        
        # Auto-extract technical skills from job description
        print(f"Extracting skills from job description...")
        must_have_skills, nice_to_have_skills = await ai_service.extract_skills_from_jd(
            job_description_text
        )
        
        print(f"Extracted must-have skills: {must_have_skills}")
        print(f"Extracted nice-to-have skills: {nice_to_have_skills}")
        
        # Create job entry with auto-extracted skills
        job_id = await cosmos_service.create_job_description(
            user_id=current_user["user_id"],
            screening_name=job_data.screening_name,
            job_description_text=job_description_text,
            must_have_skills=must_have_skills,  # List of strings
            nice_to_have_skills=nice_to_have_skills,  # List of strings
            filename=filename,
            blob_url=blob_url
        )
        
        return JobDescriptionResponse(
            job_id=job_id,
            message="Job description uploaded successfully and skills auto-extracted",
            blob_url=blob_url,
            must_have_skills=must_have_skills,
            nice_to_have_skills=nice_to_have_skills
        )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in upload_job_description: {str(e)}")
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
                
                print(f"Screening resume: {resume_file.filename}")
                print(f"Using must-have skills: {job_data['must_have_skills']}")
                print(f"Using nice-to-have skills: {job_data['nice_to_have_skills']}")
                
                # Perform AI screening
                screening_result = await ai_service.screen_candidate(
                    resume_text=resume_text,
                    job_description=job_data["job_description_text"],
                    must_have_skills=job_data["must_have_skills"],  # List of strings
                    nice_to_have_skills=job_data["nice_to_have_skills"]  # List of strings
                )
                
                print(f"Screening completed. Fit score: {screening_result['fit_score']['score']}")
                
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
        print(f"Error in screen_resumes: {str(e)}")
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
    
    Sort Options:
    - "recent": Most recent first (default)
    - "oldest": Oldest first
    - "week": Jobs from last 7 days (sorted by recent)
    - "month": Jobs from last 30 days (sorted by recent)
    - "name": Alphabetical by screening_name
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
        print(f"Error in get_jobs_with_filters: {str(e)}")
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