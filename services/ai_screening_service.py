"""
AI Screening Service using Azure OpenAI
Performs intelligent resume screening and analysis
"""

from openai import AzureOpenAI
from config import settings
import json
import re
from typing import List, Dict, Any


class AIScreeningService:
    """Service for AI-powered resume screening"""
    
    def __init__(self):
        """Initialize Azure OpenAI client"""
        self.client = AzureOpenAI(
            api_key=settings.AZURE_OPENAI_API_KEY,
            api_version=settings.AZURE_OPENAI_API_VERSION,
            azure_endpoint=settings.AZURE_OPENAI_ENDPOINT
        )
        self.deployment_name = settings.AZURE_OPENAI_DEPLOYMENT_NAME
    
    async def screen_candidate(
        self,
        resume_text: str,
        job_description: str,
        must_have_skills: List[Dict],
        nice_to_have_skills: List[Dict]
    ) -> Dict[str, Any]:
        """
        Screen candidate resume against job requirements
        
        Args:
            resume_text: Parsed resume text
            job_description: Job description text
            must_have_skills: List of must-have skills with weights
            nice_to_have_skills: List of nice-to-have skills with weights
        
        Returns:
            Comprehensive screening analysis
        """
        try:
            # Extract candidate basic info
            candidate_info = await self._extract_candidate_info(resume_text)
            
            # Analyze skills match
            skills_analysis = await self._analyze_skills_match(
                resume_text,
                must_have_skills,
                nice_to_have_skills
            )
            
            # Calculate fit score
            fit_score = await self._calculate_fit_score(
                resume_text,
                job_description,
                skills_analysis,
                must_have_skills,
                nice_to_have_skills
            )
            
            # Generate AI summary
            ai_summary = await self._generate_ai_summary(
                resume_text,
                job_description,
                skills_analysis
            )
            
            # Analyze skill depth
            skill_depth_analysis = await self._analyze_skill_depth(
                resume_text,
                skills_analysis["matched_must_have_list"],
                top_n=settings.TOP_SKILLS_FOR_DEPTH_ANALYSIS
            )
            
            # Analyze professional summary
            professional_summary = await self._analyze_professional_summary(resume_text)
            
            # Analyze company tiers
            company_tier_analysis = await self._analyze_company_tiers(resume_text)
            
            return {
                "candidate_info": candidate_info,
                "fit_score": fit_score,
                "skills_analysis": skills_analysis,
                "ai_summary": ai_summary,
                "skill_depth_analysis": skill_depth_analysis,
                "professional_summary": professional_summary,
                "company_tier_analysis": company_tier_analysis
            }
        
        except Exception as e:
            raise Exception(f"Failed to screen candidate: {str(e)}")
    
    async def _extract_candidate_info(self, resume_text: str) -> Dict[str, str]:
        """Extract basic candidate information including contact details"""
        
        prompt = f"""
        Extract the following information from this resume:
        - Full name
        - Email address
        - Phone number
        - Current/desired position/title
        - Location (city, state/country)
        - Total work experience (in format: "X years Y months")
        
        Resume (full content):
        {resume_text}
        
        Return ONLY a JSON object with keys: name, email, phone, position, location, total_experience.
        If information is not found, use "Not specified".
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert resume parser. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=800
            )
            
            content = response.choices[0].message.content.strip()
            # Remove markdown code blocks if present
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            return result
        
        except Exception as e:
            # Return default values if extraction fails
            return {
                "name": "Unknown",
                "email": "Not specified",
                "phone": "Not specified",
                "position": "Not specified",
                "location": "Not specified",
                "total_experience": "Not specified"
            }
    
    async def _analyze_skills_match(
        self,
        resume_text: str,
        must_have_skills: List[Dict],
        nice_to_have_skills: List[Dict]
    ) -> Dict[str, Any]:
        """Analyze which skills match from the resume - uses full resume text"""
        
        must_have_list = [skill["skill"] for skill in must_have_skills]
        nice_to_have_list = [skill["skill"] for skill in nice_to_have_skills]
        
        prompt = f"""
        Analyze this resume and determine which skills from the given lists are present.
        For each skill found, also estimate:
        - Proficiency level (Beginner/Intermediate/Advanced/Expert)
        - Years of experience (estimate like "2-3 years" or "5+ years")
        
        Resume (complete content):
        {resume_text}
        
        Must-have skills to check: {', '.join(must_have_list)}
        Nice-to-have skills to check: {', '.join(nice_to_have_list)}
        
        Return a JSON object with this structure:
        {{
            "must_have_matched": [
                {{
                    "skill": "skill name",
                    "found": true/false,
                    "proficiency_level": "level",
                    "years_of_experience": "estimate"
                }}
            ],
            "nice_to_have_matched": [same structure]
        }}
        
        Return ONLY valid JSON.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert technical recruiter analyzing resumes. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=3000
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            # Process results
            must_have_matched_list = []
            must_have_matched_count = 0
            
            for skill_match in result.get("must_have_matched", []):
                skill_obj = {
                    "skill": skill_match["skill"],
                    "found_in_resume": skill_match.get("found", False),
                    "proficiency_level": skill_match.get("proficiency_level"),
                    "years_of_experience": skill_match.get("years_of_experience")
                }
                must_have_matched_list.append(skill_obj)
                if skill_match.get("found", False):
                    must_have_matched_count += 1
            
            nice_to_have_matched_list = []
            nice_to_have_matched_count = 0
            
            for skill_match in result.get("nice_to_have_matched", []):
                skill_obj = {
                    "skill": skill_match["skill"],
                    "found_in_resume": skill_match.get("found", False),
                    "proficiency_level": skill_match.get("proficiency_level"),
                    "years_of_experience": skill_match.get("years_of_experience")
                }
                nice_to_have_matched_list.append(skill_obj)
                if skill_match.get("found", False):
                    nice_to_have_matched_count += 1
            
            return {
                "must_have_matched": must_have_matched_count,
                "must_have_total": len(must_have_skills),
                "nice_to_have_matched": nice_to_have_matched_count,
                "nice_to_have_total": len(nice_to_have_skills),
                "matched_must_have_list": must_have_matched_list,
                "matched_nice_to_have_list": nice_to_have_matched_list
            }
        
        except Exception as e:
            # Return empty match if analysis fails
            return {
                "must_have_matched": 0,
                "must_have_total": len(must_have_skills),
                "nice_to_have_matched": 0,
                "nice_to_have_total": len(nice_to_have_skills),
                "matched_must_have_list": [],
                "matched_nice_to_have_list": []
            }
    
    async def _calculate_fit_score(
        self,
        resume_text: str,
        job_description: str,
        skills_analysis: Dict,
        must_have_skills: List[Dict],
        nice_to_have_skills: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate overall fit score - uses full text without truncation"""
        
        # Calculate weighted skill match
        total_must_have_weight = sum(skill.get("weight", 5) for skill in must_have_skills)
        total_nice_to_have_weight = sum(skill.get("weight", 3) for skill in nice_to_have_skills)
        
        matched_must_have_weight = 0
        high_weight_skills = []
        
        for skill_match in skills_analysis["matched_must_have_list"]:
            if skill_match["found_in_resume"]:
                skill_obj = next((s for s in must_have_skills if s["skill"] == skill_match["skill"]), None)
                if skill_obj:
                    weight = skill_obj.get("weight", 5)
                    matched_must_have_weight += weight
                    if weight >= 8:  # High weight threshold
                        high_weight_skills.append(skill_match["skill"])
        
        matched_nice_to_have_weight = 0
        for skill_match in skills_analysis["matched_nice_to_have_list"]:
            if skill_match["found_in_resume"]:
                skill_obj = next((s for s in nice_to_have_skills if s["skill"] == skill_match["skill"]), None)
                if skill_obj:
                    matched_nice_to_have_weight += skill_obj.get("weight", 3)
        
        # Calculate base score from skills (70% weight)
        must_have_score = (matched_must_have_weight / total_must_have_weight * 100) if total_must_have_weight > 0 else 0
        nice_to_have_score = (matched_nice_to_have_weight / total_nice_to_have_weight * 100) if total_nice_to_have_weight > 0 else 0
        
        skills_score = (must_have_score * 0.85 + nice_to_have_score * 0.15)
        
        # Get AI assessment of overall fit (30% weight) - uses full content
        prompt = f"""
        Rate how well this candidate matches the job requirements on a scale of 0-100.
        Consider: relevant experience, job responsibilities match, career progression, industry fit.
        
        Job Description (complete):
        {job_description}
        
        Resume (complete):
        {resume_text}
        
        Return ONLY a JSON object: {{"ai_fit_score": score_number, "reasoning": "brief reason"}}
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert recruiter. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            result = json.loads(content)
            ai_score = result.get("ai_fit_score", 50)
        
        except:
            ai_score = 50
        
        # Combine scores
        final_score = int(skills_score * 0.70 + ai_score * 0.30)
        final_score = min(100, max(0, final_score))
        
        return {
            "score": final_score,
            "weighted_skills_contributing": high_weight_skills[:3]
        }
    
    async def _generate_ai_summary(
        self,
        resume_text: str,
        job_description: str,
        skills_analysis: Dict
    ) -> List[str]:
        """Generate AI summary points about the candidate - uses full text"""
        
        prompt = f"""
        Create 3-4 concise bullet points summarizing this candidate's strengths and fit for the role.
        Focus on: key skills, experience relevance, communication style, problem-solving abilities.
        
        Job Requirements (complete):
        {job_description}
        
        Resume (complete):
        {resume_text}
        
        Return ONLY a JSON array of strings: ["point 1", "point 2", "point 3", "point 4"]
        Each point should be 1-2 sentences.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert recruiter providing candidate summaries. Return only valid JSON array."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_tokens=800
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            summary_points = json.loads(content)
            return summary_points[:4]  # Maximum 4 points
        
        except Exception as e:
            return [
                "Unable to generate detailed summary due to parsing error.",
                "Please review the resume manually for comprehensive assessment."
            ]
    
    async def _analyze_skill_depth(
        self,
        resume_text: str,
        matched_skills: List[Dict],
        top_n: int = 6
    ) -> List[Dict[str, Any]]:
        """Analyze proficiency depth for top skills - uses full resume text"""
        
        # Get top skills that were found
        found_skills = [s for s in matched_skills if s["found_in_resume"]][:top_n]
        
        if not found_skills:
            return []
        
        skills_list = [s["skill"] for s in found_skills]
        
        prompt = f"""
        For each skill, estimate the candidate's proficiency percentage (0-100) based on their resume.
        Consider: years of experience, project complexity, leadership/mentorship, certifications.
        
        Skills to analyze: {', '.join(skills_list)}
        
        Resume (complete content):
        {resume_text}
        
        Return ONLY a JSON array:
        [
            {{"skill_name": "skill", "proficiency_percentage": number, "evidence": "brief evidence"}},
            ...
        ]
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert at assessing technical skills. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            # Ensure percentages are valid
            for item in result:
                item["proficiency_percentage"] = min(100, max(0, item.get("proficiency_percentage", 50)))
            
            return result
        
        except Exception as e:
            # Return default values
            return [
                {
                    "skill_name": skill["skill"],
                    "proficiency_percentage": 50,
                    "evidence": "Unable to assess automatically"
                }
                for skill in found_skills
            ]
    
    async def _analyze_professional_summary(self, resume_text: str) -> Dict[str, Any]:
        """Analyze professional summary including tenure, gaps, industry exposure - uses full resume"""
        
        prompt = f"""
        Analyze this resume and provide:
        1. Average job tenure (format: "X years Y months")
        2. Tenure assessment (Low/Moderate/High/Very High based on average tenure)
        3. Career gap if any (duration and reason if mentioned)
        4. Industry exposure percentages (identify top industries and their percentage distribution)
        5. Total number of companies worked for
        
        Resume (complete content):
        {resume_text}
        
        Return ONLY a JSON object:
        {{
            "average_job_tenure": "X years Y months",
            "tenure_assessment": "Low/Moderate/High/Very High",
            "career_gap": {{"duration": "X years Y months", "reason": "reason or null"}},
            "industry_exposure": [
                {{"industry": "name", "percentage": number}},
                ...
            ],
            "total_companies": number
        }}
        
        For career_gap, return null if no significant gap found.
        Industry percentages should sum to 100.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing career histories. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            # Structure the response
            return {
                "average_job_tenure": result.get("average_job_tenure", "Not specified"),
                "tenure_assessment": result.get("tenure_assessment", "Moderate"),
                "career_gap": result.get("career_gap"),
                "major_industry_exposure": result.get("industry_exposure", []),
                "total_companies": result.get("total_companies", 0)
            }
        
        except Exception as e:
            return {
                "average_job_tenure": "Not specified",
                "tenure_assessment": "Moderate",
                "career_gap": None,
                "major_industry_exposure": [],
                "total_companies": 0
            }
    
    async def _analyze_company_tiers(self, resume_text: str) -> Dict[str, int]:
        """Analyze distribution of company tiers (Startup/Mid-size/Enterprise) - uses full resume"""
        
        prompt = f"""
        Analyze the companies mentioned in this resume and classify them into:
        - Startup (small companies, typically <100 employees)
        - Mid-size (medium companies, 100-1000 employees)
        - Enterprise (large corporations, >1000 employees)
        
        Provide percentage distribution that sums to 100.
        
        Resume (complete content):
        {resume_text}
        
        Return ONLY a JSON object:
        {{
            "startup_percentage": number,
            "mid_size_percentage": number,
            "enterprise_percentage": number
        }}
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing companies. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=400
            )
            
            content = response.choices[0].message.content.strip()
            content = re.sub(r'```json\n?|\n?```', '', content)
            
            result = json.loads(content)
            
            # Ensure percentages sum to 100
            total = result.get("startup_percentage", 0) + result.get("mid_size_percentage", 0) + result.get("enterprise_percentage", 0)
            
            if total == 0:
                return {
                    "startup_percentage": 33,
                    "mid_size_percentage": 34,
                    "enterprise_percentage": 33
                }
            
            # Normalize to 100
            factor = 100 / total
            return {
                "startup_percentage": int(result.get("startup_percentage", 0) * factor),
                "mid_size_percentage": int(result.get("mid_size_percentage", 0) * factor),
                "enterprise_percentage": int(result.get("enterprise_percentage", 0) * factor)
            }
        
        except Exception as e:
            return {
                "startup_percentage": 33,
                "mid_size_percentage": 34,
                "enterprise_percentage": 33
            }