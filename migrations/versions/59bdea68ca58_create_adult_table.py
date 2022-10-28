"""create_adult_table

Revision ID: 59bdea68ca58
Revises: 
Create Date: 2022-10-28 00:08:40.271765

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '59bdea68ca58'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'adults',
        sa.Column('id',sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column('age', sa.Integer, nullable=False),
        sa.Column('workclass', sa.String(50), nullable=False),
        sa.Column('fnlwgt', sa.Integer, nullable=False),
        sa.Column('education', sa.String(30), nullable=False),
        sa.Column('education-num', sa.Integer, nullable=False),
        sa.Column('marital-status', sa.String(50), nullable=False),
        sa.Column('occupation', sa.String(50), nullable=False),
        sa.Column('relationship', sa.String(50), nullable=False),
        sa.Column('race', sa.String(50), nullable=False),
        sa.Column('sex', sa.String(50), nullable=False),
        sa.Column('capital-gain', sa.Integer, nullable=False),
        sa.Column('capital-loss', sa.Integer, nullable=False),
        sa.Column('hours-per-week', sa.Integer, nullable=False),
        sa.Column('native-country', sa.String(120), nullable=False),
        sa.Column('class', sa.String(50), nullable=False)
    )


def downgrade() -> None:
    op.drop_table("adults")
