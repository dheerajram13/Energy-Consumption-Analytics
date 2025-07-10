import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import pandas as pd

from .client import PowerBIClient, PowerBIConfig, create_energy_dataset_schema
from ..models.energy_models import EnergyConsumption, Anomaly, PowerPlant
from ..config.database import get_db_session

logger = logging.getLogger(__name__)

class PowerBIService:
    """
    Service class for managing PowerBI integration with energy analytics platform
    """
    
    def __init__(self, config: PowerBIConfig, db_session: Optional[Session] = None):
        """
        Initialize PowerBI service
        
        Args:
            config: PowerBI configuration
            db_session: Database session (optional)
        """
        self.client = PowerBIClient(config)
        self.db = db_session or next(get_db_session())
        self.workspace_name = "Energy Analytics Platform"
        self.dataset_name = "Energy Consumption Data"
        self.workspace_id = None
        self.dataset_id = None
        
    def initialize_powerbi_workspace(self) -> bool:
        """
        Initialize PowerBI workspace and dataset for energy analytics
        
        Returns:
            bool: True if successful
        """
        try:
            # Authenticate with PowerBI
            if not self.client.authenticate():
                logger.error("Failed to authenticate with PowerBI")
                return False
            
            # Get or create workspace
            workspace = self.client.get_workspace_by_name(self.workspace_name)
            if not workspace:
                logger.info(f"Creating PowerBI workspace: {self.workspace_name}")
                workspace = self.client.create_workspace(self.workspace_name)
            
            self.workspace_id = workspace["id"]
            logger.info(f"Using PowerBI workspace: {self.workspace_name} (ID: {self.workspace_id})")
            
            # Get or create dataset
            datasets = self.client.get_datasets(self.workspace_id)
            existing_dataset = next((ds for ds in datasets if ds["name"] == self.dataset_name), None)
            
            if not existing_dataset:
                logger.info(f"Creating PowerBI dataset: {self.dataset_name}")
                schema = create_energy_dataset_schema(self.dataset_name)
                dataset = self.client.create_dataset(schema, self.workspace_id)
                self.dataset_id = dataset["id"]
            else:
                self.dataset_id = existing_dataset["id"]
                logger.info(f"Using existing PowerBI dataset: {self.dataset_name} (ID: {self.dataset_id})")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize PowerBI workspace: {str(e)}")
            return False
    
    def sync_energy_consumption_data(self, start_date: Optional[datetime] = None, 
                                   end_date: Optional[datetime] = None,
                                   clear_existing: bool = False) -> bool:
        """
        Sync energy consumption data from database to PowerBI
        
        Args:
            start_date: Start date for data sync (None for all data)
            end_date: End date for data sync (None for current time)
            clear_existing: Whether to clear existing data first
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.workspace_id or not self.dataset_id:
                if not self.initialize_powerbi_workspace():
                    return False
            
            # Clear existing data if requested
            if clear_existing:
                logger.info("Clearing existing energy consumption data in PowerBI")
                self.client.clear_dataset_table(
                    self.dataset_id, 
                    "EnergyConsumption", 
                    self.workspace_id
                )
            
            # Query energy consumption data
            query = self.db.query(EnergyConsumption)
            
            if start_date:
                query = query.filter(EnergyConsumption.timestamp >= start_date)
            if end_date:
                query = query.filter(EnergyConsumption.timestamp <= end_date)
            
            # Get data in batches to avoid memory issues
            batch_size = 1000
            offset = 0
            total_synced = 0
            
            while True:
                batch_data = query.offset(offset).limit(batch_size).all()
                
                if not batch_data:
                    break
                
                # Convert to PowerBI format
                powerbi_data = []
                for record in batch_data:
                    powerbi_data.append({
                        "Timestamp": record.timestamp.isoformat(),
                        "Region": record.region or "Unknown",
                        "ConsumptionMWh": float(record.consumption_mwh or 0),
                        "Temperature": float(record.temperature or 0),
                        "IsHoliday": bool(record.is_holiday),
                        "HourOfDay": record.timestamp.hour,
                        "DayOfWeek": record.timestamp.weekday(),
                        "Month": record.timestamp.month,
                        "Year": record.timestamp.year,
                        "Season": self._get_season(record.timestamp.month)
                    })
                
                # Push to PowerBI
                self.client.push_data_to_dataset(
                    self.dataset_id,
                    "EnergyConsumption",
                    powerbi_data,
                    self.workspace_id
                )
                
                total_synced += len(powerbi_data)
                offset += batch_size
                
                logger.info(f"Synced {total_synced} energy consumption records to PowerBI")
            
            logger.info(f"Energy consumption data sync completed. Total records: {total_synced}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync energy consumption data: {str(e)}")
            return False
    
    def sync_anomaly_data(self, start_date: Optional[datetime] = None,
                         end_date: Optional[datetime] = None,
                         clear_existing: bool = False) -> bool:
        """
        Sync anomaly data from database to PowerBI
        
        Args:
            start_date: Start date for data sync
            end_date: End date for data sync
            clear_existing: Whether to clear existing data first
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.workspace_id or not self.dataset_id:
                if not self.initialize_powerbi_workspace():
                    return False
            
            if clear_existing:
                logger.info("Clearing existing anomaly data in PowerBI")
                self.client.clear_dataset_table(
                    self.dataset_id,
                    "Anomalies",
                    self.workspace_id
                )
            
            # Query anomaly data
            query = self.db.query(Anomaly)
            
            if start_date:
                query = query.filter(Anomaly.timestamp >= start_date)
            if end_date:
                query = query.filter(Anomaly.timestamp <= end_date)
            
            anomalies = query.all()
            
            if anomalies:
                powerbi_data = []
                for anomaly in anomalies:
                    powerbi_data.append({
                        "Timestamp": anomaly.timestamp.isoformat(),
                        "Region": anomaly.region or "Unknown",
                        "ActualValue": float(anomaly.actual_value or 0),
                        "PredictedValue": float(anomaly.predicted_value or 0),
                        "AnomalyScore": float(anomaly.anomaly_score or 0),
                        "IsConfirmed": bool(anomaly.is_confirmed),
                        "DetectionMethod": "IsolationForest"  # Default method
                    })
                
                # Push to PowerBI
                self.client.push_data_to_dataset(
                    self.dataset_id,
                    "Anomalies",
                    powerbi_data,
                    self.workspace_id
                )
                
                logger.info(f"Synced {len(powerbi_data)} anomaly records to PowerBI")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync anomaly data: {str(e)}")
            return False
    
    def sync_power_plant_data(self, clear_existing: bool = False) -> bool:
        """
        Sync power plant data from database to PowerBI
        
        Args:
            clear_existing: Whether to clear existing data first
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.workspace_id or not self.dataset_id:
                if not self.initialize_powerbi_workspace():
                    return False
            
            if clear_existing:
                logger.info("Clearing existing power plant data in PowerBI")
                self.client.clear_dataset_table(
                    self.dataset_id,
                    "PowerPlants",
                    self.workspace_id
                )
            
            # Query power plant data
            power_plants = self.db.query(PowerPlant).all()
            
            if power_plants:
                powerbi_data = []
                for plant in power_plants:
                    powerbi_data.append({
                        "Id": plant.id,
                        "Name": plant.name or "Unknown",
                        "Region": plant.region or "Unknown",
                        "CapacityMW": float(plant.capacity_mw or 0),
                        "FuelType": plant.fuel_type or "Unknown",
                        "CommissioningYear": 2020,  # Default value
                        "Latitude": 0.0,  # Would need to add these fields to model
                        "Longitude": 0.0
                    })
                
                # Push to PowerBI
                self.client.push_data_to_dataset(
                    self.dataset_id,
                    "PowerPlants",
                    powerbi_data,
                    self.workspace_id
                )
                
                logger.info(f"Synced {len(powerbi_data)} power plant records to PowerBI")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync power plant data: {str(e)}")
            return False
    
    def push_forecast_data(self, forecast_data: List[Dict]) -> bool:
        """
        Push forecast data to PowerBI
        
        Args:
            forecast_data: List of forecast dictionaries
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.workspace_id or not self.dataset_id:
                if not self.initialize_powerbi_workspace():
                    return False
            
            # Clear existing forecasts
            self.client.clear_dataset_table(
                self.dataset_id,
                "Forecasts",
                self.workspace_id
            )
            
            # Format data for PowerBI
            powerbi_data = []
            for forecast in forecast_data:
                powerbi_data.append({
                    "Timestamp": forecast["timestamp"],
                    "Region": forecast.get("region", "Unknown"),
                    "ForecastValue": float(forecast["forecast_value"]),
                    "ConfidenceIntervalLow": float(forecast.get("confidence_low", 0)),
                    "ConfidenceIntervalHigh": float(forecast.get("confidence_high", 0)),
                    "ModelType": forecast.get("model_type", "Prophet"),
                    "CreatedAt": datetime.utcnow().isoformat()
                })
            
            # Push to PowerBI
            if powerbi_data:
                self.client.push_data_to_dataset(
                    self.dataset_id,
                    "Forecasts",
                    powerbi_data,
                    self.workspace_id
                )
                logger.info(f"Pushed {len(powerbi_data)} forecast records to PowerBI")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to push forecast data: {str(e)}")
            return False
    
    def _get_season(self, month: int) -> str:
        """
        Get season based on month
        
        Args:
            month: Month number (1-12)
            
        Returns:
            str: Season name
        """
        if 3 <= month <= 5:
            return "Spring"
        elif 6 <= month <= 8:
            return "Summer"
        elif 9 <= month <= 11:
            return "Autumn"
        else:
            return "Winter"
    
    def refresh_dataset(self) -> bool:
        """
        Trigger a refresh of the PowerBI dataset
        
        Returns:
            bool: True if successful
        """
        try:
            if not self.workspace_id or not self.dataset_id:
                if not self.initialize_powerbi_workspace():
                    return False
            
            logger.info(f"Triggering refresh for dataset ID: {self.dataset_id}")
            self.client.refresh_dataset(self.dataset_id, self.workspace_id)
            logger.info("Dataset refresh triggered successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to refresh dataset: {str(e)}")
            return False

    def get_refresh_status(self) -> Optional[Dict[str, Any]]:
        """
        Get the refresh status of the dataset
        
        Returns:
            Optional[Dict]: Refresh status information or None if not available
        """
        try:
            if not self.workspace_id or not self.dataset_id:
                if not self.initialize_powerbi_workspace():
                    return None
            
            # Get refresh history (most recent first)
            refreshes = self.client.get_refresh_history(self.dataset_id, self.workspace_id, 1)
            
            if refreshes and len(refreshes) > 0:
                return refreshes[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting refresh status: {str(e)}")
            return None
    
    def sync_all_data(self, clear_existing: bool = True, 
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None) -> bool:
        """
        Sync all data to PowerBI
        
        Args:
            clear_existing: Whether to clear existing data first
            start_date: Optional start date for data sync
            end_date: Optional end date for data sync
            
        Returns:
            bool: True if all syncs were successful
        """
        try:
            # Initialize workspace if needed
            if not self.workspace_id or not self.dataset_id:
                if not self.initialize_powerbi_workspace():
                    return False
            
            # Sync all data types
            results = [
                self.sync_energy_consumption_data(
                    start_date=start_date,
                    end_date=end_date,
                    clear_existing=clear_existing
                ),
                self.sync_anomaly_data(
                    start_date=start_date,
                    end_date=end_date,
                    clear_existing=clear_existing
                ),
                self.sync_power_plant_data(clear_existing=clear_existing)
            ]
            
            # Refresh dataset to make sure all data is available
            if all(results):
                return self.refresh_dataset()
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to sync all data: {str(e)}")
            return False
