from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID
from enum import Enum
from sqlalchemy import TIMESTAMP, Column, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class AlertStatus(str, Enum):
    """Current status of operational alerts."""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class OpsAlertManagement(SQLModel, table=False):
    """
    Operational alert management dashboard.
    
    Centralized view of all active alerts, their status, and resolution tracking
    for operations teams to manage incidents and maintain system health.
    
    SQL View Definition:
    CREATE OR REPLACE VIEW ops_gold.ops_alert_management AS
    SELECT 
        a.alert_id,
        a.alert_timestamp,
        a.alert_category,
        a.severity,
        a.alert_name,
        a.alert_description,
        
        -- Status and lifecycle
        CASE 
            WHEN a.resolved_at IS NOT NULL THEN 'resolved'::text
            WHEN a.acknowledged_at IS NOT NULL THEN 'acknowledged'::text
            WHEN a.is_active = false THEN 'suppressed'::text
            ELSE 'active'::text
        END as current_status,
        
        -- Timing information
        EXTRACT(EPOCH FROM (COALESCE(a.resolved_at, CURRENT_TIMESTAMP) - a.alert_timestamp))/60 
            as duration_minutes,
        EXTRACT(EPOCH FROM (COALESCE(a.acknowledged_at, CURRENT_TIMESTAMP) - a.alert_timestamp))/60 
            as time_to_acknowledge_minutes,
        
        -- Context and assignment
        a.store_id,
        a.affected_system,
        a.acknowledged_by,
        a.resolution_notes,
        
        -- Escalation tracking
        a.escalation_level,
        a.notification_sent,
        
        -- Source and details
        a.source_metric,
        a.source_value,
        a.threshold_value,
        a.alert_context,
        
        -- Priority scoring (for operations teams)
        CASE 
            WHEN a.severity = 'critical' AND a.escalation_level >= 2 THEN 100
            WHEN a.severity = 'critical' AND a.escalation_level = 1 THEN 90
            WHEN a.severity = 'critical' THEN 80
            WHEN a.severity = 'warning' AND a.escalation_level >= 1 THEN 70
            WHEN a.severity = 'warning' THEN 60
            ELSE 50
        END as priority_score
        
    FROM ops_bronze.ops_alert_events a
    WHERE a.alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    ORDER BY priority_score DESC, a.alert_timestamp DESC;
    """
    
    # Alert identification
    alert_id: UUID = Field(primary_key=True)
    alert_timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True)),
        description="When the alert was first raised"
    )
    
    # Alert classification
    alert_category: str = Field(max_length=50, description="Category of the alert")
    severity: str = Field(max_length=10, description="Alert severity level")
    alert_name: str = Field(max_length=100, description="Alert name/title")
    alert_description: str = Field(max_length=500, description="Alert description")
    
    # Current status
    current_status: AlertStatus = Field(
        sa_column=Column(SQLEnum(AlertStatus)),
        description="Current alert status"
    )
    
    # Timing metrics
    duration_minutes: Optional[float] = Field(
        description="Total duration of alert in minutes"
    )
    time_to_acknowledge_minutes: Optional[float] = Field(
        description="Time taken to acknowledge alert"
    )
    
    # Context and ownership
    store_id: Optional[int] = Field(description="Affected store ID")
    affected_system: str = Field(max_length=50, description="System affected by alert")
    acknowledged_by: Optional[str] = Field(max_length=50, description="Who acknowledged the alert")
    resolution_notes: Optional[str] = Field(max_length=1000, description="Resolution notes")
    
    # Escalation information
    escalation_level: int = Field(description="Current escalation level")
    notification_sent: bool = Field(description="Whether notifications were sent")
    
    # Technical details
    source_metric: Optional[str] = Field(max_length=100, description="Source metric that triggered alert")
    source_value: Optional[float] = Field(description="Value that triggered the alert")
    threshold_value: Optional[float] = Field(description="Threshold that was breached")
    alert_context: Dict[str, Any] = Field(
        sa_column=Column(JSONB),
        description="Additional context for the alert"
    )
    
    # Priority for operations
    priority_score: int = Field(description="Calculated priority score for operations teams")
    
    # SLA tracking
    sla_breach: Optional[bool] = Field(
        description="Whether this alert represents an SLA breach"
    )
    business_impact: Optional[str] = Field(
        max_length=20, 
        description="Business impact level: low, medium, high"
    )
    
    # Resolution tracking
    estimated_resolution_time: Optional[int] = Field(
        description="Estimated resolution time in minutes"
    )
    actual_resolution_time: Optional[int] = Field(
        description="Actual resolution time in minutes"
    )