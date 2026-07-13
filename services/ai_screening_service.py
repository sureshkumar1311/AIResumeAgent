"""
AI Screening Service using Azure OpenAI
Performs intelligent resume screening and analysis.

FIX: Scoring now applies a mathematical hard cap based on must-have skill
match ratio BEFORE returning the final score. The AI can no longer inflate
scores using generic/soft factors when critical skills are missing.

Scoring logic:
  - Must-have skills control the ceiling of the final score
  - 0% must-have match  → final score cannot exceed 30
  - 50% must-have match → final score cannot exceed 65
  - 100% must-have match → final score can reach 100
  - AI evaluates experience, domain relevance, and trajectory
    but only within the ceiling set by must-have match ratio
"""

from openai import AzureOpenAI
from config import settings
import json
import re
from typing import List, Dict, Any, Tuple


class AIScreeningService:

    def __init__(self):
        self.client = AzureOpenAI(
            api_key       = settings.AZURE_OPENAI_API_KEY,
            api_version   = settings.AZURE_OPENAI_API_VERSION,
            azure_endpoint= settings.AZURE_OPENAI_ENDPOINT,
        )
        self.deployment_name = settings.AZURE_OPENAI_DEPLOYMENT_NAME

    # ── Public methods ────────────────────────────────────────────────────────

    async def extract_skills_from_jd(
        self, job_description_text: str
    ) -> Tuple[List[str], List[str]]:
        """Extract must-have and nice-to-have technical skills from a JD."""

        prompt = f"""
        Analyze this job description and extract ONLY technical skills, tools,
        technologies, and programming languages.

        RULES:
        1. Extract ONLY technical skills (languages, frameworks, tools, platforms, databases)
        2. DO NOT include: years of experience, soft skills, education, certifications
        3. Categorize:
           - must_have_skills: Core technical requirements explicitly stated as required/mandatory
           - nice_to_have_skills: Preferred/bonus technical skills

        Job Description:
        {job_description_text}

        Return ONLY a JSON object:
        {{
            "must_have_skills": ["skill1", "skill2", ...],
            "nice_to_have_skills": ["skill1", "skill2", ...]
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You are an expert at analysing job descriptions. Return only valid JSON."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 2000,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            result  = json.loads(content)
            return result.get("must_have_skills", []), result.get("nice_to_have_skills", [])

        except Exception as e:
            print(f"Error extracting skills: {e}")
            return [], []

    async def screen_candidate(
        self,
        resume_text:         str,
        job_description:     str,
        must_have_skills:    List[str],
        nice_to_have_skills: List[str],
    ) -> Dict[str, Any]:
        """Screen candidate resume against job requirements."""

        try:
            candidate_info       = await self._extract_candidate_info(resume_text)
            skills_analysis      = await self._analyze_skills_match(
                resume_text, must_have_skills, nice_to_have_skills
            )
            fit_score            = await self._calculate_fit_score(
                resume_text, job_description, skills_analysis
            )
            ai_summary           = await self._generate_ai_summary(
                resume_text, job_description, skills_analysis
            )
            skill_depth_analysis = await self._analyze_skill_depth(
                resume_text,
                skills_analysis["matched_must_have_list"],
                top_n=settings.TOP_SKILLS_FOR_DEPTH_ANALYSIS,
            )
            professional_summary = await self._analyze_professional_summary(resume_text)
            company_tier_analysis= await self._analyze_company_tiers(resume_text)

            return {
                "candidate_info":       candidate_info,
                "fit_score":            fit_score,
                "skills_analysis":      skills_analysis,
                "ai_summary":           ai_summary,
                "skill_depth_analysis": skill_depth_analysis,
                "professional_summary": professional_summary,
                "company_tier_analysis":company_tier_analysis,
            }

        except Exception as e:
            raise Exception(f"Failed to screen candidate: {e}")

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _extract_candidate_info(self, resume_text: str) -> Dict[str, str]:

        prompt = f"""
        Extract the following from this resume:
        - Full name
        - Email address
        - Phone number
        - Current/desired position
        - Location (city, state/country)
        - Total work experience (format: "X years Y months")

        Resume:
        {resume_text}

        Return ONLY a JSON object with keys:
        name, email, phone, position, location, total_experience.
        Use "Not specified" if not found.
        """

        try:
            response = self.client.chat.completions.create(
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You are an expert resume parser. Return only valid JSON."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 800,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            return json.loads(content)

        except Exception:
            return {
                "name": "Unknown", "email": "Not specified",
                "phone": "Not specified", "position": "Not specified",
                "location": "Not specified", "total_experience": "Not specified",
            }

    async def _analyze_skills_match(
        self,
        resume_text:         str,
        must_have_skills:    List[str],
        nice_to_have_skills: List[str],
    ) -> Dict[str, Any]:

        prompt = f"""
        Analyse this resume and determine which skills from the given lists are present.

        INSTRUCTIONS:
        1. Mark a skill as "found": true ONLY if there is CLEAR evidence in the resume.
        2. Accept variations: "React.js" matches "React", "Python3" matches "Python".
        3. Look in work experience, projects, skills sections, and certifications.
        4. For each found skill, estimate proficiency and years of experience.

        Proficiency levels:
        - Beginner:     0-1 years
        - Intermediate: 1-3 years
        - Advanced:     3-5 years
        - Expert:       5+ years

        Resume:
        {resume_text}

        Must-have skills:    {', '.join(must_have_skills) if must_have_skills else 'None'}
        Nice-to-have skills: {', '.join(nice_to_have_skills) if nice_to_have_skills else 'None'}

        Return ONLY valid JSON:
        {{
            "must_have_matched": [
                {{
                    "skill": "skill name",
                    "found": true/false,
                    "proficiency_level": "Beginner/Intermediate/Advanced/Expert",
                    "years_of_experience": "0-1 years"
                }}
            ],
            "nice_to_have_matched": [ ...same structure... ]
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You are an expert technical recruiter. Return only valid JSON. Be consistent and thorough."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 4000,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            result  = json.loads(content)

            must_have_matched_list  = []
            must_have_matched_count = 0

            for item in result.get("must_have_matched", []):
                must_have_matched_list.append({
                    "skill":              item["skill"],
                    "found_in_resume":    item.get("found", False),
                    "proficiency_level":  item.get("proficiency_level"),
                    "years_of_experience":item.get("years_of_experience"),
                })
                if item.get("found", False):
                    must_have_matched_count += 1

            nice_to_have_matched_list  = []
            nice_to_have_matched_count = 0

            for item in result.get("nice_to_have_matched", []):
                nice_to_have_matched_list.append({
                    "skill":              item["skill"],
                    "found_in_resume":    item.get("found", False),
                    "proficiency_level":  item.get("proficiency_level"),
                    "years_of_experience":item.get("years_of_experience"),
                })
                if item.get("found", False):
                    nice_to_have_matched_count += 1

            return {
                "must_have_matched":         must_have_matched_count,
                "must_have_total":           len(must_have_skills),
                "nice_to_have_matched":      nice_to_have_matched_count,
                "nice_to_have_total":        len(nice_to_have_skills),
                "matched_must_have_list":    must_have_matched_list,
                "matched_nice_to_have_list": nice_to_have_matched_list,
            }

        except Exception:
            return {
                "must_have_matched":         0,
                "must_have_total":           len(must_have_skills),
                "nice_to_have_matched":      0,
                "nice_to_have_total":        len(nice_to_have_skills),
                "matched_must_have_list":    [],
                "matched_nice_to_have_list": [],
            }

    # ── FIXED SCORING ─────────────────────────────────────────────────────────

    def _calculate_must_have_ceiling(
        self,
        must_have_matched: int,
        must_have_total:   int,
    ) -> int:
        """
        Derives a hard score ceiling from must-have skill match ratio.
        The AI score is capped to this value regardless of other factors.

        Ceiling formula:
            ceiling = 30 + (70 * ratio)

        Examples:
             0 / 10 matched → ratio 0.00 → ceiling  30
             1 / 10 matched → ratio 0.10 → ceiling  37
             3 / 10 matched → ratio 0.30 → ceiling  51
             5 / 10 matched → ratio 0.50 → ceiling  65
             7 / 10 matched → ratio 0.70 → ceiling  79
            10 / 10 matched → ratio 1.00 → ceiling 100

        If no must-have skills were defined (must_have_total == 0),
        the ceiling is unrestricted (100) so the AI score stands alone.
        """
        if must_have_total == 0:
            return 100

        ratio   = must_have_matched / must_have_total
        ceiling = int(30 + (70 * ratio))
        return min(ceiling, 100)

    async def _calculate_fit_score(
        self,
        resume_text:     str,
        job_description: str,
        skills_analysis: Dict,
    ) -> Dict[str, Any]:
        """
        Two-stage scoring:
          1. AI evaluates the full candidate profile holistically.
          2. A mathematical ceiling derived from must-have skill ratio
             is applied — the AI score cannot exceed this ceiling.

        This prevents generic experience / soft skills from compensating
        for missing critical technical requirements.
        """

        must_have_matched = skills_analysis["must_have_matched"]
        must_have_total   = skills_analysis["must_have_total"]
        ceiling           = self._calculate_must_have_ceiling(must_have_matched, must_have_total)

        # ── Stage 1: AI holistic evaluation ──────────────────────────────────
        prompt = f"""
        You are a strict technical recruiter scoring a candidate against a job description.

        CRITICAL SCORING RULES — you MUST follow these:

        1. MUST-HAVE SKILLS are the most important factor.
           - The candidate has matched {must_have_matched} out of {must_have_total} required skills.
           - If fewer than half the must-have skills are present, the score MUST be below 50.
           - If NO must-have skills are present, the score MUST be below 30.
           - Generic skills (MS Office, communication, teamwork) must NEVER compensate
             for missing domain-specific technical requirements.

        2. DOMAIN EXPERIENCE must be directly relevant, not tangential.
           - A candidate from a completely different industry with no relevant projects
             should score low even if they have many years of total experience.

        3. SCORING BANDS:
           85-100 : Meets nearly all must-have skills AND has directly relevant experience
           70-84  : Meets most must-have skills (>70%) with relevant domain experience
           55-69  : Meets roughly half the must-have skills, some domain relevance
           40-54  : Meets fewer than half must-have skills, limited domain relevance
           25-39  : Meets very few must-have skills, different domain
           0-24   : Meets almost no must-have skills, wrong domain entirely

        EVALUATION FACTORS:
        - Must-have technical skills match (50% weight)
        - Domain-specific experience relevance (25% weight)
        - Years of experience vs requirement (15% weight)
        - Nice-to-have skills + career trajectory (10% weight)

        Job Description:
        {job_description}

        Resume:
        {resume_text}

        Must-have skills matched: {must_have_matched} of {must_have_total}
        Nice-to-have matched:     {skills_analysis['nice_to_have_matched']} of {skills_analysis['nice_to_have_total']}

        Return ONLY a JSON object:
        {{
            "score": <integer 0-100>,
            "reasoning": "<2-3 sentences explaining the score, mentioning specific skill gaps>"
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You are a strict technical recruiter. Missing critical skills must significantly lower the score. Return only valid JSON."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 500,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            result  = json.loads(content)

            ai_score = min(100, max(0, int(result.get("score", 50))))
            reasoning = result.get("reasoning", "Score based on overall profile match.")

        except Exception as e:
            print(f"Error in AI scoring: {e}")
            ai_score  = 50
            reasoning = "Unable to calculate detailed fit score. Manual review recommended."

        # ── Stage 2: Apply hard ceiling from must-have ratio ──────────────────
        final_score = min(ai_score, ceiling)

        # Append a note to reasoning if the ceiling changed the score
        if final_score < ai_score:
            reasoning += (
                f" Score adjusted from {ai_score} to {final_score} because only "
                f"{must_have_matched}/{must_have_total} critical skills were matched "
                f"(maximum allowed: {ceiling})."
            )

        return {
            "score":     final_score,
            "reasoning": reasoning,
        }

    # ── Remaining methods unchanged ───────────────────────────────────────────

    async def _generate_ai_summary(
        self,
        resume_text:     str,
        job_description: str,
        skills_analysis: Dict,
    ) -> List[str]:

        prompt = f"""
        Create 3-4 concise bullet points summarising this candidate's strengths and
        fit for the role. Be objective and mention specific skill gaps if present.

        Job Requirements:
        {job_description}

        Resume:
        {resume_text}

        Must-have skills matched: {skills_analysis['must_have_matched']} of {skills_analysis['must_have_total']}

        Return ONLY a JSON array of strings: ["point 1", "point 2", "point 3"]
        Each point should be 1-2 sentences based on factual resume content.
        """

        try:
            response = self.client.chat.completions.create(
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You are an expert recruiter. Return only a valid JSON array with at least 3 items."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0.1,
                max_tokens  = 800,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            points  = json.loads(content)

            if not points or len(points) < 3:
                raise ValueError("Too few summary points")

            return points[:4]

        except Exception:
            sa = skills_analysis
            return [
                f"Matched {sa['must_have_matched']} of {sa['must_have_total']} required technical skills.",
                f"Matched {sa['nice_to_have_matched']} of {sa['nice_to_have_total']} preferred skills.",
                "Please review the detailed resume for a comprehensive assessment.",
            ]

    async def _analyze_skill_depth(
        self,
        resume_text:    str,
        matched_skills: List[Dict],
        top_n:          int = 6,
    ) -> List[Dict[str, Any]]:

        found_skills = [s for s in matched_skills if s["found_in_resume"]][:top_n]
        if not found_skills:
            return []

        skills_list = [s["skill"] for s in found_skills]

        prompt = f"""
        For each skill, estimate the candidate's proficiency percentage (0-100).

        GUIDELINES:
        0-25%:   Beginner — mentioned briefly, minimal experience
        26-50%:  Intermediate — used in 1-2 projects, 1-2 years
        51-75%:  Advanced — used extensively, 3-5 years, led projects
        76-100%: Expert — deep expertise, 5+ years, mentored others

        Skills to analyse: {', '.join(skills_list)}

        Resume:
        {resume_text}

        Return ONLY a JSON array:
        [
            {{"skill_name": "skill", "proficiency_percentage": number, "evidence": "brief evidence"}},
            ...
        ]
        """

        try:
            response = self.client.chat.completions.create(
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You assess technical skills objectively. Return only valid JSON."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 3000,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            result  = json.loads(content)

            for item in result:
                item["proficiency_percentage"] = min(100, max(0, item.get("proficiency_percentage", 50)))

            return result

        except Exception:
            return [
                {"skill_name": s["skill"], "proficiency_percentage": 50, "evidence": "Unable to assess"}
                for s in found_skills
            ]

    async def _analyze_professional_summary(self, resume_text: str) -> Dict[str, Any]:

        prompt = f"""
        Analyse this resume and provide:
        1. Average job tenure (format: "X years Y months")
        2. Tenure assessment: Low / Moderate / High / Very High
        3. Career gap if any (duration + reason if mentioned), or null
        4. Industry exposure percentages (top industries, must sum to 100)
        5. Total number of companies worked for

        Resume:
        {resume_text}

        Return ONLY a JSON object:
        {{
            "average_job_tenure": "X years Y months",
            "tenure_assessment": "Low/Moderate/High/Very High",
            "career_gap": {{"duration": "X months", "reason": "reason"}} or null,
            "industry_exposure": [{{"industry": "name", "percentage": number}}],
            "total_companies": number
        }}

        Return null for career_gap if no significant gap (>6 months) is found.
        """

        try:
            response = self.client.chat.completions.create(
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You analyse career histories. Return only valid JSON."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 1500,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            result  = json.loads(content)

            career_gap = result.get("career_gap")
            if career_gap and not isinstance(career_gap.get("duration"), str):
                career_gap = None

            return {
                "average_job_tenure":    result.get("average_job_tenure", "Not specified"),
                "tenure_assessment":     result.get("tenure_assessment", "Moderate"),
                "career_gap":            career_gap,
                "major_industry_exposure": result.get("industry_exposure", []),
                "total_companies":       result.get("total_companies", 0),
            }

        except Exception as e:
            print(f"Error analysing professional summary: {e}")
            return {
                "average_job_tenure": "Not specified",
                "tenure_assessment":  "Moderate",
                "career_gap":         None,
                "major_industry_exposure": [],
                "total_companies":    0,
            }

    async def _analyze_company_tiers(self, resume_text: str) -> Dict[str, int]:

        prompt = f"""
        Classify the companies in this resume into:
        - Startup    (<100 employees)
        - Mid-size   (100-1000 employees)
        - Enterprise (>1000 employees)

        Percentages must sum to 100.

        Resume:
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
                model    = self.deployment_name,
                messages = [
                    {"role": "system", "content": "You analyse companies. Return only valid JSON."},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 400,
            )
            content = re.sub(r'```json\n?|\n?```', '', response.choices[0].message.content.strip())
            result  = json.loads(content)

            startup    = result.get("startup_percentage", 0)
            mid        = result.get("mid_size_percentage", 0)
            enterprise = result.get("enterprise_percentage", 0)
            total      = startup + mid + enterprise

            if total == 0:
                return {"startup_percentage": 33, "mid_size_percentage": 34, "enterprise_percentage": 33}

            factor = 100 / total
            return {
                "startup_percentage":    int(startup    * factor),
                "mid_size_percentage":   int(mid        * factor),
                "enterprise_percentage": int(enterprise * factor),
            }

        except Exception:
            return {"startup_percentage": 33, "mid_size_percentage": 34, "enterprise_percentage": 33}