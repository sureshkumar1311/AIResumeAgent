"""
Azure Service Bus service for processing resume screening messages
"""

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import ServiceBusError
from config import settings
import json
from datetime import datetime


class ServiceBusService:
    """Service for Azure Service Bus operations"""
    
    def __init__(self):
        """Initialize Service Bus client"""
        self.connection_string = settings.AZURE_SERVICE_BUS_CONNECTION_STRING
        self.queue_name = settings.AZURE_SERVICE_BUS_QUEUE_NAME
    
    async def send_resume_for_processing(
        self,
        job_id: str,
        resume_blob_url: str,
        resume_filename: str,
        container_name: str = "resumes"
    ) -> bool:
        """
        Send resume processing message to Service Bus queue
        
        Args:
            job_id: Job description ID
            resume_blob_url: Azure Blob URL of the resume
            resume_filename: Original filename
            container_name: Blob container name
        
        Returns:
            True if message sent successfully
        """
        try:
            # Create message payload
            message_body = {
                "job_id": job_id,
                "resume_blob_url": resume_blob_url,
                "resume_filename": resume_filename,
                "container_name": container_name,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Send message to queue
            with ServiceBusClient.from_connection_string(
                self.connection_string
            ) as client:
                with client.get_queue_sender(self.queue_name) as sender:
                    message = ServiceBusMessage(
                        body=json.dumps(message_body),
                        content_type="application/json"
                    )
                    sender.send_messages(message)
            
            print(f" Sent resume to queue: {resume_filename} for job: {job_id}")
            return True
        
        except ServiceBusError as e:
            print(f" Service Bus error: {str(e)}")
            return False
        except Exception as e:
            print(f" Error sending message to Service Bus: {str(e)}")
            return False
    
    async def process_resume_from_blob_event(
        self,
        blob_url: str,
        blob_name: str
    ) -> bool:
        """
        Process resume directly from Event Grid blob created event
        This extracts metadata from blob path and sends to processing queue
        
        Expected blob path format: resumes/{screening_job_id}/{user_id}/{timestamp}_{filename}
        
        Args:
            blob_url: Complete blob URL
            blob_name: Blob path (e.g., "resumes/job-123/user-456/resume.pdf")
        
        Returns:
            True if message sent successfully
        """
        try:
            # Parse blob path to extract metadata
            # Format: resumes/{screening_job_id}/{user_id}/{timestamp}_{filename}
            parts = blob_name.split('/')
            
            if len(parts) < 4 or parts[0] != "resumes":
                print(f" Invalid blob path format: {blob_name}")
                return False
            
            screening_job_id = parts[1]
            user_id = parts[2]
            filename = parts[3]
            
            # Get job_id from screening_job metadata in Cosmos DB
            # (We'll need to look this up)
            from services.cosmos_db_service import CosmosDBService
            cosmos_service = CosmosDBService()
            
            screening_job = await cosmos_service.get_screening_job(screening_job_id)
            if not screening_job:
                print(f" Screening job not found: {screening_job_id}")
                return False
            
            job_id = screening_job.get("job_id")
            
            # Send to processing queue
            return await self.send_resume_for_processing(
                screening_job_id=screening_job_id,
                job_id=job_id,
                user_id=user_id,
                resume_blob_url=blob_url,
                resume_filename=filename
            )
        
        except Exception as e:
            print(f" Error processing blob event: {str(e)}")
            return False