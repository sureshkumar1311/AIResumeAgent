"""
AI Resume Screener - FastAPI Backend
Handles job description upload and resume screening with detailed AI analysis
"""

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import json
from datetime import datetime

from models import (
    JobDescriptionResponse,
    ResumeScreeningResponse,
    CandidateReport
)
from services.azure_blob_service import AzureBlobService
from services.document_parser import DocumentParser
from services.ai_screening_service import AIScreeningService
from services.cosmos_db_service import CosmosDBService
from config import settings

app = FastAPI(
    title="AI Resume Screener API",
    description="Intelligent resume screening system with Azure OpenAI",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
blob_service = AzureBlobService()
document_parser = DocumentParser()
ai_service = AIScreeningService()
cosmos_service = CosmosDBService()


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "AI Resume Screener",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/api/job-description", response_model=JobDescriptionResponse)
async def upload_job_description(
    job_description_file: UploadFile = File(...),
    must_have_skills: str = Form(...),
    nice_to_have_skills: str = Form(...)
):
    """
    Upload job description and skills
    
    Args:
        job_description_file: PDF or Word document containing job description
        must_have_skills: JSON array of skill strings ["Python", "FastAPI", ...]
        nice_to_have_skills: JSON array of skill strings ["Docker", "Kubernetes", ...]
    
    Returns:
        JobDescriptionResponse with job_id and uploaded file details
    """
    try:
        # Validate file type
        if not job_description_file.filename.endswith(('.pdf', '.docx', '.doc')):
            raise HTTPException(
                status_code=400,
                detail="Invalid file format. Only PDF and Word documents are supported."
            )
        
        # Parse skills from JSON (now expecting simple string arrays)
        try:
            must_have_skills_list = json.loads(must_have_skills)
            nice_to_have_skills_list = json.loads(nice_to_have_skills)
            
            # Convert simple strings to skill objects with default weights
            # Must-have skills get weight 8, Nice-to-have skills get weight 5
            must_have_skills_objects = [
                {"skill": skill, "weight": 8} for skill in must_have_skills_list
            ]
            nice_to_have_skills_objects = [
                {"skill": skill, "weight": 5} for skill in nice_to_have_skills_list
            ]
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400,
                detail="Invalid JSON format for skills. Expected: [\"skill1\", \"skill2\"]"
            )
        
        # Upload job description to blob storage
        file_content = await job_description_file.read()
        blob_url = await blob_service.upload_file(
            file_content,
            f"job-descriptions/{datetime.utcnow().timestamp()}_{job_description_file.filename}",
            content_type=job_description_file.content_type
        )
        
        # Parse job description text
        job_description_text = await document_parser.parse_document(
            file_content,
            job_description_file.filename
        )
        
        # Create job entry in CosmosDB
        job_id = await cosmos_service.create_job_description(
            job_description_text=job_description_text,
            blob_url=blob_url,
            must_have_skills=must_have_skills_objects,
            nice_to_have_skills=nice_to_have_skills_objects,
            filename=job_description_file.filename
        )
        
        return JobDescriptionResponse(
            job_id=job_id,
            message="Job description uploaded successfully",
            blob_url=blob_url,
            must_have_skills_count=len(must_have_skills_list),
            nice_to_have_skills_count=len(nice_to_have_skills_list)
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/screen-resumes", response_model=ResumeScreeningResponse)
async def screen_resumes(
    job_id: str = Form(...),
    resumes: List[UploadFile] = File(...)
):
    """
    Screen single or multiple resumes against job description
    
    Args:
        job_id: ID of the job description from previous endpoint
        resumes: List of resume files (PDF or Word)
    
    Returns:
        ResumeScreeningResponse with detailed screening reports for all candidates
    """
    try:
        if not resumes:
            raise HTTPException(
                status_code=400,
                detail="At least one resume file is required"
            )
        
        # Validate file types
        for resume in resumes:
            if not resume.filename.endswith(('.pdf', '.docx', '.doc')):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid file format for {resume.filename}. Only PDF and Word documents are supported."
                )
        
        # Retrieve job description from CosmosDB
        job_data = await cosmos_service.get_job_description(job_id)
        if not job_data:
            raise HTTPException(
                status_code=404,
                detail="Job description not found"
            )
        
        candidate_reports = []
        
        # Process each resume
        for resume_file in resumes:
            try:
                # Read resume content
                resume_content = await resume_file.read()
                
                # Upload resume to blob storage
                resume_blob_url = await blob_service.upload_file(
                    resume_content,
                    f"resumes/{job_id}/{datetime.utcnow().timestamp()}_{resume_file.filename}",
                    content_type=resume_file.content_type
                )
                
                # Parse resume text
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
                
                # Save screening result to CosmosDB
                await cosmos_service.save_screening_result(
                    job_id=job_id,
                    candidate_report=candidate_report.dict()
                )
                
                candidate_reports.append(candidate_report)
                
            except Exception as e:
                # Log error but continue with other resumes
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


@app.get("/api/screening-result/{job_id}")
async def get_screening_results(job_id: str):
    """
    Retrieve all screening results for a job
    
    Args:
        job_id: ID of the job description
    
    Returns:
        All screening results for the job
    """
    try:
        results = await cosmos_service.get_screening_results(job_id)
        if not results:
            raise HTTPException(
                status_code=404,
                detail="No screening results found for this job"
            )
        
        return {
            "job_id": job_id,
            "total_candidates": len(results),
            "results": results
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)