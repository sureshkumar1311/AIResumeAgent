"""
Pydantic models for API request and response validation
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Union
from datetime import datetime


class JobDescriptionResponse(BaseModel):
    """Job description upload response"""
    job_id: str = Field(..., description="Unique job ID")
    message: str
    blob_url: str = Field(..., description="Azure Blob Storage URL for job description")
    must_have_skills_count: int
    nice_to_have_skills_count: int


class FitScore(BaseModel):
    """Fit score details"""
    score: int = Field(..., ge=0, le=100, description="Overall fit score percentage")
    weighted_skills_contributing: List[str] = Field(
        default=[],
        description="Skills that contributed more weightage to the score"
    )


class MatchedSkill(BaseModel):
    """Matched skill details"""
    skill: str
    found_in_resume: bool
    proficiency_level: Optional[str] = None
    years_of_experience: Optional[str] = None


class SkillsAnalysis(BaseModel):
    """Skills matching analysis"""
    must_have_matched: int
    must_have_total: int
    nice_to_have_matched: int
    nice_to_have_total: int
    matched_must_have_list: List[MatchedSkill]
    matched_nice_to_have_list: List[MatchedSkill]


class SkillDepth(BaseModel):
    """Skill depth analysis for individual skill"""
    skill_name: str
    proficiency_percentage: int = Field(..., ge=0, le=100)
    evidence: Optional[str] = Field(
        None,
        description="Brief evidence from resume supporting this proficiency"
    )


class CareerGap(BaseModel):
    """Career gap details"""
    duration: str = Field(..., description="Duration of gap (e.g., '2 years 3 months')")
    reason: Optional[str] = Field(None, description="Reason for career gap if mentioned")


class IndustryExposure(BaseModel):
    """Industry exposure details"""
    industry: str
    percentage: int = Field(..., ge=0, le=100)


class ProfessionalSummary(BaseModel):
    """Professional summary details"""
    average_job_tenure: str = Field(..., description="Average tenure (e.g., '3 years 6 months')")
    tenure_assessment: str = Field(
        ...,
        description="Assessment of tenure stability (Low/Moderate/High/Very High)"
    )
    career_gap: Optional[CareerGap] = None
    major_industry_exposure: List[IndustryExposure]
    total_companies: int


class CompanyTierAnalysis(BaseModel):
    """Company tier distribution"""
    startup_percentage: int = Field(..., ge=0, le=100)
    mid_size_percentage: int = Field(..., ge=0, le=100)
    enterprise_percentage: int = Field(..., ge=0, le=100)


class CandidateInfo(BaseModel):
    """Basic candidate information"""
    name: str
    position: str
    location: Optional[str] = None
    total_experience: str


class CandidateReport(BaseModel):
    """Complete candidate screening report"""
    candidate_name: str
    email: Optional[str] = Field(None, description="Candidate email address")
    phone: Optional[str] = Field(None, description="Candidate phone number")
    position: str
    location: Optional[str] = None
    total_experience: str
    resume_url: str = Field(..., description="Azure Blob Storage URL for resume")
    resume_filename: str
    
    # Fit Score
    fit_score: FitScore
    
    # Skills Analysis
    must_have_skills_matched: int
    must_have_skills_total: int
    nice_to_have_skills_matched: int
    nice_to_have_skills_total: int
    matched_must_have_skills: List[MatchedSkill]
    matched_nice_to_have_skills: List[MatchedSkill]
    
    # AI Summary (3-4 bullet points)
    ai_summary: List[str] = Field(
        ...,
        description="AI-generated summary points about the candidate",
        min_items=3,
        max_items=5
    )
    
    # Skill Depth Analysis
    skill_depth_analysis: List[SkillDepth] = Field(
        ...,
        description="Detailed analysis of top 6-8 skills"
    )
    
    # Professional Summary
    professional_summary: ProfessionalSummary
    
    # Company Tier Analysis
    company_tier_analysis: CompanyTierAnalysis


class ResumeScreeningResponse(BaseModel):
    """Response for resume screening endpoint"""
    job_id: str
    total_resumes_processed: int
    candidates: List[CandidateReport]
    processing_timestamp: str


class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    detail: str
    timestamp: str