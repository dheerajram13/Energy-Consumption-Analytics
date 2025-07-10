import os
import requests
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import pandas as pd
import logging
from dataclasses import dataclass
from enum import Enum
import msal

logger = logging.getLogger(__name__)

class DatasetRefreshStatus(Enum):
    """Enum for dataset refresh status"""
    UNKNOWN = "Unknown"
    COMPLETED = "Completed" 
    FAILED = "Failed"
    DISABLED = "Disabled"
    IN_PROGRESS = "InProgress"

@dataclass
class PowerBIConfig:
    """Configuration for PowerBI API"""
    tenant_id: str
    client_id: str
    client_secret: str
    username: Optional[str] = None
    password: Optional[str] = None
    authority: str = "https://login.microsoftonline.com"
    scope: List[str] = None
    
    def __post_init__(self):
        if self.scope is None:
            self.scope = ["https://analysis.windows.net/powerbi/api/.default"]

@dataclass
class Dataset:
    """PowerBI Dataset representation"""
    id: str
    name: str
    workspace_id: str
    is_refreshable: bool = False
    configured_by: Optional[str] = None
    created_date: Optional[datetime] = None
    
class PowerBIClient:
    """
    PowerBI REST API Client for managing datasets, reports, and dashboards
    """
    
    def __init__(self, config: PowerBIConfig):
        """
        Initialize PowerBI client
        
        Args:
            config: PowerBI configuration object
        """
        self.config = config
        self.base_url = "https://api.powerbi.com/v1.0/myorg"
        self.token = None
        self.token_expires = None
        self._session = requests.Session()
        
    def authenticate(self) -> bool:
        """
        Authenticate with PowerBI using Azure AD
        
        Returns:
            bool: True if authentication successful
        """
        try:
            authority_url = f"{self.config.authority}/{self.config.tenant_id}"
            
            # Initialize MSAL app
            app = msal.ConfidentialClientApplication(
                client_id=self.config.client_id,
                client_credential=self.config.client_secret,
                authority=authority_url
            )
            
            # Try to get token from cache first
            accounts = app.get_accounts()
            if accounts:
                result = app.acquire_token_silent(
                    scopes=self.config.scope,
                    account=accounts[0]
                )
            else:
                result = None
            
            # If no cached token, acquire new token
            if not result:
                if self.config.username and self.config.password:
                    # Username/password flow (for service accounts)
                    result = app.acquire_token_by_username_password(
                        username=self.config.username,
                        password=self.config.password,
                        scopes=self.config.scope
                    )
                else:
                    # Client credentials flow (for app-only access)
                    result = app.acquire_token_for_client(
                        scopes=self.config.scope
                    )
            
            if "access_token" in result:
                self.token = result["access_token"]
                # Calculate token expiry (usually 1 hour)
                expires_in = result.get("expires_in", 3600)
                self.token_expires = datetime.now() + timedelta(seconds=expires_in)
                
                # Update session headers
                self._session.headers.update({
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                })
                
                logger.info("PowerBI authentication successful")
                return True
            else:
                logger.error(f"PowerBI authentication failed: {result.get('error_description', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"PowerBI authentication error: {str(e)}")
            return False
    
    def _ensure_authenticated(self) -> bool:
        """
        Ensure we have a valid token
        
        Returns:
            bool: True if authenticated
        """
        if not self.token or (self.token_expires and datetime.now() >= self.token_expires):
            return self.authenticate()
        return True
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> requests.Response:
        """
        Make authenticated request to PowerBI API
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request data
            params: URL parameters
            
        Returns:
            Response object
        """
        if not self._ensure_authenticated():
            raise Exception("Failed to authenticate with PowerBI")
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self._session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=30
            )
            
            if response.status_code == 401:
                # Token might be expired, try to re-authenticate
                if self.authenticate():
                    response = self._session.request(
                        method=method,
                        url=url,
                        json=data,
                        params=params,
                        timeout=30
                    )
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"PowerBI API request failed: {str(e)}")
            raise
    
    # ========== Workspace Operations ==========
    
    def get_workspaces(self) -> List[Dict]:
        """
        Get all workspaces (groups) the user has access to
        
        Returns:
            List of workspace dictionaries
        """
        response = self._make_request("GET", "/groups")
        return response.json().get("value", [])
    
    def get_workspace_by_name(self, name: str) -> Optional[Dict]:
        """
        Get workspace by name
        
        Args:
            name: Workspace name
            
        Returns:
            Workspace dictionary or None
        """
        workspaces = self.get_workspaces()
        for workspace in workspaces:
            if workspace.get("name") == name:
                return workspace
        return None
    
    def create_workspace(self, name: str) -> Dict:
        """
        Create a new workspace
        
        Args:
            name: Workspace name
            
        Returns:
            Created workspace dictionary
        """
        data = {"name": name}
        response = self._make_request("POST", "/groups", data=data)
        return response.json()
    
    # ========== Dataset Operations ==========
    
    def get_datasets(self, workspace_id: Optional[str] = None) -> List[Dict]:
        """
        Get datasets from workspace or personal workspace
        
        Args:
            workspace_id: Workspace ID (None for personal workspace)
            
        Returns:
            List of dataset dictionaries
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets"
        else:
            endpoint = "/datasets"
        
        response = self._make_request("GET", endpoint)
        return response.json().get("value", [])
    
    def get_dataset(self, dataset_id: str, workspace_id: Optional[str] = None) -> Dict:
        """
        Get specific dataset details
        
        Args:
            dataset_id: Dataset ID
            workspace_id: Workspace ID (None for personal workspace)
            
        Returns:
            Dataset dictionary
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}"
        else:
            endpoint = f"/datasets/{dataset_id}"
        
        response = self._make_request("GET", endpoint)
        return response.json()
    
    def create_dataset(self, dataset_schema: Dict, workspace_id: Optional[str] = None) -> Dict:
        """
        Create a new dataset
        
        Args:
            dataset_schema: Dataset schema definition
            workspace_id: Workspace ID (None for personal workspace)
            
        Returns:
            Created dataset dictionary
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets"
        else:
            endpoint = "/datasets"
        
        response = self._make_request("POST", endpoint, data=dataset_schema)
        return response.json()
    
    def delete_dataset(self, dataset_id: str, workspace_id: Optional[str] = None) -> bool:
        """
        Delete a dataset
        
        Args:
            dataset_id: Dataset ID
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}"
        else:
            endpoint = f"/datasets/{dataset_id}"
        
        self._make_request("DELETE", endpoint)
        return True
    
    def refresh_dataset(self, dataset_id: str, workspace_id: Optional[str] = None, notify_option: str = "MailOnFailure") -> bool:
        """
        Trigger dataset refresh
        
        Args:
            dataset_id: Dataset ID
            workspace_id: Workspace ID
            notify_option: Notification option (MailOnFailure, MailOnCompletion, NoNotification)
            
        Returns:
            True if refresh triggered successfully
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        else:
            endpoint = f"/datasets/{dataset_id}/refreshes"
        Args:
            dataset_id: Dataset ID
            table_name: Table name
            data: List of row dictionaries
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows"
        else:
            endpoint = f"/datasets/{dataset_id}/tables/{table_name}/rows"
        
        # PowerBI expects data in "rows" format
        payload = {"rows": data}
        self._make_request("POST", endpoint, data=payload)
        return True
    
    def clear_dataset_table(self, dataset_id: str, table_name: str, workspace_id: Optional[str] = None) -> bool:
        """
        Clear all rows from a dataset table
        
        Args:
            dataset_id: Dataset ID
            table_name: Table name
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows"
        else:
            endpoint = f"/datasets/{dataset_id}/tables/{table_name}/rows"
        
        self._make_request("DELETE", endpoint)
        return True
    
    # ========== Report Operations ==========
    
    def get_reports(self, workspace_id: Optional[str] = None) -> List[Dict]:
        """
        Get reports from workspace
        
        Args:
            workspace_id: Workspace ID (None for personal workspace)
            
        Returns:
            List of report dictionaries
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/reports"
        else:
            endpoint = "/reports"
        
        response = self._make_request("GET", endpoint)
        return response.json().get("value", [])
    
    def clone_report(self, report_id: str, new_name: str, target_workspace_id: Optional[str] = None, source_workspace_id: Optional[str] = None) -> Dict:
        """
        Clone a report
        
        Args:
            report_id: Source report ID
            new_name: New report name
            target_workspace_id: Target workspace ID
            source_workspace_id: Source workspace ID
            
        Returns:
            Cloned report dictionary
        """
        data = {"name": new_name}
        if target_workspace_id:
            data["targetWorkspaceId"] = target_workspace_id
        
        if source_workspace_id:
            endpoint = f"/groups/{source_workspace_id}/reports/{report_id}/Clone"
        else:
            endpoint = f"/reports/{report_id}/Clone"
        
        response = self._make_request("POST", endpoint, data=data)
        return response.json()
    
    # ========== Dashboard Operations ==========
    
    def get_dashboards(self, workspace_id: Optional[str] = None) -> List[Dict]:
        """
        Get dashboards from workspace
        
        Args:
            workspace_id: Workspace ID (None for personal workspace)
            
        Returns:
            List of dashboard dictionaries
        """
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/dashboards"
        else:
            endpoint = "/dashboards"
        
        response = self._make_request("GET", endpoint)
        return response.json().get("value", [])
    
    # ========== Data Export Operations ==========
    
    def export_report_to_file(self, report_id: str, file_format: str = "PDF", workspace_id: Optional[str] = None) -> bytes:
        """
        Export report to file
        
        Args:
            report_id: Report ID
            file_format: Export format (PDF, PNG, PPTX, etc.)
            workspace_id: Workspace ID
            
        Returns:
            File content as bytes
        """
        data = {"format": file_format}
        
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/reports/{report_id}/Export"
        else:
            endpoint = f"/reports/{report_id}/Export"
        
        response = self._make_request("POST", endpoint, data=data)
        export_id = response.json()["id"]
        
        # Poll for completion
        while True:
            if workspace_id:
                status_endpoint = f"/groups/{workspace_id}/reports/{report_id}/exports/{export_id}"
            else:
                status_endpoint = f"/reports/{report_id}/exports/{export_id}"
            
            status_response = self._make_request("GET", status_endpoint)
            status_data = status_response.json()
            
            if status_data["status"] == "Succeeded":
                # Download the file
                file_response = self._make_request("GET", f"{status_endpoint}/file")
                return file_response.content
            elif status_data["status"] == "Failed":
                raise Exception(f"Export failed: {status_data.get('error', 'Unknown error')}")
            
            # Wait before polling again
            import time
            time.sleep(5)

def create_energy_dataset_schema(dataset_name: str) -> Dict:
    """
    Create schema for energy consumption dataset
    
    Args:
        dataset_name: Name of the dataset
        
    Returns:
        Dataset schema dictionary
    """
    return {
        "name": dataset_name,
        "defaultMode": "Push",
        "tables": [
            {
                "name": "EnergyConsumption",
                "columns": [
                    {"name": "Timestamp", "dataType": "DateTime"},
                    {"name": "Region", "dataType": "String"},
                    {"name": "ConsumptionMWh", "dataType": "Double"},
                    {"name": "Temperature", "dataType": "Double"},
                    {"name": "IsHoliday", "dataType": "Boolean"},
                    {"name": "HourOfDay", "dataType": "Int64"},
                    {"name": "DayOfWeek", "dataType": "Int64"},
                    {"name": "Month", "dataType": "Int64"},
                    {"name": "Year", "dataType": "Int64"},
                    {"name": "Season", "dataType": "String"}
                ]
            },
            {
                "name": "Anomalies",
                "columns": [
                    {"name": "Timestamp", "dataType": "DateTime"},
                    {"name": "Region", "dataType": "String"},
                    {"name": "ActualValue", "dataType": "Double"},
                    {"name": "PredictedValue", "dataType": "Double"},
                    {"name": "AnomalyScore", "dataType": "Double"},
                    {"name": "IsConfirmed", "dataType": "Boolean"},
                    {"name": "DetectionMethod", "dataType": "String"}
                ]
            },
            {
                "name": "Forecasts",
                "columns": [
                    {"name": "Timestamp", "dataType": "DateTime"},
                    {"name": "Region", "dataType": "String"},
                    {"name": "ForecastValue", "dataType": "Double"},
                    {"name": "ConfidenceIntervalLow", "dataType": "Double"},
                    {"name": "ConfidenceIntervalHigh", "dataType": "Double"},
                    {"name": "ModelType", "dataType": "String"},
                    {"name": "CreatedAt", "dataType": "DateTime"}
                ]
            },
            {
                "name": "PowerPlants",
                "columns": [
                    {"name": "Id", "dataType": "Int64"},
                    {"name": "Name", "dataType": "String"},
                    {"name": "Region", "dataType": "String"},
                    {"name": "CapacityMW", "dataType": "Double"},
                    {"name": "FuelType", "dataType": "String"},
                    {"name": "CommissioningYear", "dataType": "Int64"},
                    {"name": "Latitude", "dataType": "Double"},
                    {"name": "Longitude", "dataType": "Double"}
                ]
            }
        ]
    }
