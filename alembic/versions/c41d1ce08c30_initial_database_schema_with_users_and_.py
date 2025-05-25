"""Initial database schema with users and energy models

Revision ID: c41d1ce08c30
Revises: 
Create Date: 2025-05-25 22:05:14.516549

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c41d1ce08c30'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create users table
    op.create_table('users',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('username', sa.String(), nullable=False),
        sa.Column('email', sa.String(), nullable=True),
        sa.Column('hashed_password', sa.String(), nullable=False),
        sa.Column('full_name', sa.String(), nullable=True),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('is_superuser', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes with IF NOT EXISTS
    op.execute('CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username ON users (username)')
    op.execute('CREATE INDEX IF NOT EXISTS ix_users_email ON users (email) WHERE email IS NOT NULL')
    
    # Create power_plants table
    op.create_table('power_plants',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('location', sa.String(), nullable=True),
        sa.Column('capacity_mw', sa.Float(), nullable=True),
        sa.Column('plant_type', sa.String(), nullable=True),
        sa.Column('owner_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create energy_consumption table
    op.create_table('energy_consumption',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('power_plant_id', sa.Integer(), sa.ForeignKey('power_plants.id', ondelete='CASCADE'), nullable=False),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('energy_consumed_kwh', sa.Float(), nullable=False),
        sa.Column('temperature_c', sa.Float(), nullable=True),
        sa.Column('humidity_percent', sa.Float(), nullable=True),
        sa.Column('is_holiday', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.execute('CREATE INDEX IF NOT EXISTS ix_energy_consumption_timestamp ON energy_consumption (timestamp)')
    
    # Create anomalies table
    op.create_table('anomalies',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('energy_consumption_id', sa.Integer(), sa.ForeignKey('energy_consumption.id', ondelete='CASCADE'), nullable=False),
        sa.Column('anomaly_score', sa.Float(), nullable=False),
        sa.Column('detected_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('is_resolved', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('resolved_at', sa.DateTime(), nullable=True),
        sa.Column('resolved_by', sa.Integer(), sa.ForeignKey('users.id', ondelete='SET NULL'), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

def downgrade() -> None:
    # Drop all tables in reverse order of creation
    op.drop_table('anomalies')
    
    # Drop indexes with IF EXISTS
    op.execute('DROP INDEX IF EXISTS ix_energy_consumption_timestamp')
    op.drop_table('energy_consumption')
    op.drop_table('power_plants')
    
    # Drop user indexes and table
    op.execute('DROP INDEX IF EXISTS ix_users_email')
    op.execute('DROP INDEX IF EXISTS ix_users_username')
    op.drop_table('users')
