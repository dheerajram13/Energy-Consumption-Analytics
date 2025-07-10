from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import logging

from ...powerbi.service import PowerBIService
from ...powerbi.client import PowerBIConfig
from ...config.database import get_db_session
from ...auth.auth_utils import get_current_active_user
from ...models.energy_models import User
from ...config.settings import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/powerbi", tags=["powerbi"])

# Pydantic models
class PowerBISyncRequest(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    clear_existing: bool = False
    data_types: List[str] = Field(default=["energy", "anomalies", "power_plants"])

class PowerBIRefreshRequest(BaseModel):
    notify_option: str = Field(default="MailOnFailure", regex="^(MailOnFailure|MailOnCompletion|NoNotification)$")

class ForecastDataRequest(BaseModel):
    forecasts: List[Dict[str, Any]]

class PowerBIExportRequest(BaseModel):
    report_name: str
    file_format: str = Field(default="PDF", regex="^(PDF|PNG|PPTX)$")

class PowerBIStatusResponse(BaseModel):
    workspace_id: Optional[str]
    dataset_id: Optional[str]
    workspace_name: str
    dataset_name: str
    last_refresh: Optional[datetime]
    refresh_status: Optional[str]

# Dependency to get PowerBI service
def get_powerbi_service(db: Session = Depends(get_db_session)) -> PowerBIService:
    """
    Get PowerBI service instance
    
    Args:
        db: Database session
        
    Returns:
        PowerBIService instance
    """
    # Create PowerBI configuration from settings
    config = PowerBIConfig(
        tenant_id=settings.POWERBI_TENANT_ID,
        client_id=settings.POWERBI_CLIENT_ID,
        client_secret=settings.POWERBI_CLIENT_SECRET,
        username=getattr(settings, 'POWERBI_USERNAME', None),
        password=getattr(settings, 'POWERBI_PASSWORD', None)
    )
    
    return PowerBIService(config, db)

@router.get("/status", response_model=PowerBIStatusResponse)
async def get_powerbi_status(
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get PowerBI integration status
    """
    try:
        # Initialize workspace if needed
        if not powerbi_service.workspace_id:
            powerbi_service.initialize_powerbi_workspace()
        
        # Get refresh status
        refresh_status = powerbi_service.get_refresh_status()
        
        return PowerBIStatusResponse(
            workspace_id=powerbi_service.workspace_id,
            dataset_id=powerbi_service.dataset_id,
            workspace_name=powerbi_service.workspace_name,
            dataset_name=powerbi_service.dataset_name,
            last_refresh=datetime.fromisoformat(refresh_status["endTime"].replace("Z", "+00:00")) if refresh_status and "endTime" in refresh_status else None,
            refresh_status=refresh_status["status"] if refresh_status else None
        )
        
    except Exception as e:
        logger.error(f"Error getting PowerBI status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get PowerBI status: {str(e)}"
        )

@router.post("/initialize")
async def initialize_powerbi_workspace(
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Initialize PowerBI workspace and dataset
    """
    try:
        success = powerbi_service.initialize_powerbi_workspace()
        
        if success:
            return {
                "message": "PowerBI workspace initialized successfully",
                "workspace_id": powerbi_service.workspace_id,
                "dataset_id": powerbi_service.dataset_id
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to initialize PowerBI workspace"
            )
            
    except Exception as e:
        logger.error(f"Error initializing PowerBI workspace: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/sync")
async def sync_data_to_powerbi(
    request: PowerBISyncRequest,
    background_tasks: BackgroundTasks,
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Sync data to PowerBI (runs in background)
    """
    try:
        def sync_task():
            """Background task for data synchronization"""
            try:
                if "all" in request.data_types:
                    success = powerbi_service.sync_all_data(
                        start_date=request.start_date,
                        end_date=request.end_date,
                        clear_existing=request.clear_existing
                    )
                else:
                    success = True
                    
                    if "energy" in request.data_types:
                        success &= powerbi_service.sync_energy_consumption_data(
                            start_date=request.start_date,
                            end_date=request.end_date,
                            clear_existing=request.clear_existing
                        )
                    
                    if "anomalies" in request.data_types:
                        success &= powerbi_service.sync_anomaly_data(
                            start_date=request.start_date,
                            end_date=request.end_date,
                            clear_existing=request.clear_existing
                        )
                    
                    if "power_plants" in request.data_types:
                        success &= powerbi_service.sync_power_plant_data(
                            clear_existing=request.clear_existing
                        )
                
                logger.info(f"PowerBI sync completed. Success: {success}")
                
            except Exception as e:
                logger.error(f"PowerBI sync failed: {str(e)}")
        
        # Add to background tasks
        background_tasks.add_task(sync_task)
        
        return {
            "message": "Data synchronization started in background",
            "data_types": request.data_types,
            "start_date": request.start_date,
            "end_date": request.end_date
        }
        
    except Exception as e:
        logger.error(f"Error starting PowerBI sync: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/sync/energy")
async def sync_energy_data(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    clear_existing: bool = Query(False),
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Sync energy consumption data to PowerBI
    """
    try:
        success = powerbi_service.sync_energy_consumption_data(
            start_date=start_date,
            end_date=end_date,
            clear_existing=clear_existing
        )
        
        if success:
            return {"message": "Energy consumption data synced successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to sync energy consumption data"
            )
            
    except Exception as e:
        logger.error(f"Error syncing energy data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/sync/anomalies")
async def sync_anomaly_data(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    clear_existing: bool = Query(False),
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Sync anomaly data to PowerBI
    """
    try:
        success = powerbi_service.sync_anomaly_data(
            start_date=start_date,
            end_date=end_date,
            clear_existing=clear_existing
        )
        
        if success:
            return {"message": "Anomaly data synced successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to sync anomaly data"
            )
            
    except Exception as e:
        logger.error(f"Error syncing anomaly data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/forecasts")
async def push_forecast_data(
    request: ForecastDataRequest,
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Push forecast data to PowerBI
    """
    try:
        success = powerbi_service.push_forecast_data(request.forecasts)
        
        if success:
            return {
                "message": "Forecast data pushed successfully",
                "forecast_count": len(request.forecasts)
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to push forecast data"
            )
            
    except Exception as e:
        logger.error(f"Error pushing forecast data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/refresh")
async def refresh_dataset(
    request: PowerBIRefreshRequest,
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Trigger PowerBI dataset refresh
    """
    try:
        success = powerbi_service.refresh_dataset()
        
        if success:
            return {"message": "Dataset refresh triggered successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to trigger dataset refresh"
            )
            
    except Exception as e:
        logger.error(f"Error refreshing dataset: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/refresh/status")
async def get_refresh_status(
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get dataset refresh status
    """
    try:
        refresh_status = powerbi_service.get_refresh_status()
        
        if refresh_status:
            return {
                "status": refresh_status.get("status"),
                "start_time": refresh_status.get("startTime"),
                "end_time": refresh_status.get("endTime"),
                "request_id": refresh_status.get("requestId")
            }
        else:
            return {"message": "No refresh history found"}
            
    except Exception as e:
        logger.error(f"Error getting refresh status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/export")
async def export_report(
    request: PowerBIExportRequest,
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Export PowerBI report to file
    """
    try:
        # First, get the report ID by name
        reports = powerbi_service.client.get_reports()
        report = next((r for r in reports if r["name"] == request.report_name), None)
        
        if not report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Report '{request.report_name}' not found"
            )
            
        # Export the report
        file_content = powerbi_service.client.export_report(
            report_id=report["id"],
            file_format=request.file_format,
            report_name=request.report_name
        )
        
        if file_content:
            # Return file content as base64 for JSON response
            import base64
            
            return {
                "message": "Report exported successfully",
                "file_content": base64.b64encode(file_content).decode('utf-8'),
                "file_format": request.file_format,
                "report_name": request.report_name
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to export report"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting report: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/workspaces")
async def get_workspaces(
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get all PowerBI workspaces
    """
    try:
        workspaces = powerbi_service.client.get_workspaces()
        
        return {
            "workspaces": [
                {
                    "id": ws["id"],
                    "name": ws["name"],
                    "type": ws.get("type", "Unknown")
                }
                for ws in workspaces
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting workspaces: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/datasets")
async def get_datasets(
    workspace_id: Optional[str] = Query(None),
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get PowerBI datasets
    """
    try:
        datasets = powerbi_service.client.get_datasets(workspace_id)
        
        return {
            "datasets": [
                {
                    "id": ds["id"],
                    "name": ds["name"],
                    "configured_by": ds.get("configuredBy"),
                    "is_refreshable": ds.get("isRefreshable", False),
                    "created_date": ds.get("createdDate")
                }
                for ds in datasets
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting datasets: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/reports")
async def get_reports(
    workspace_id: Optional[str] = Query(None),
    powerbi_service: PowerBIService = Depends(get_powerbi_service),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get PowerBI reports
    """
    try:
        reports = powerbi_service.client.get_reports(workspace_id)
        
        return {
            "reports": [
                {
                    "id": report["id"],
                    "name": report["name"],
                    "web_url": report.get("webUrl"),
                    "embed_url": report.get("embedUrl")
                }
                for report in reports
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting reports: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
