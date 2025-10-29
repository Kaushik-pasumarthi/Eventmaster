pip install -r requirements.txt

## Run this in PostgreSQL
GRANT ALL PRIVILEGES ON DATABASE corporate_actions_db TO prowess_user;
GRANT ALL ON SCHEMA public TO prowess_user;
ALTER DATABASE corporate_actions_db OWNER TO prowess_user;
ALTER USER prowess_user WITH PASSWORD ' ';

# Edit .env with your values
DATABASE_URL=postgresql://your_app_user:secure_password@localhost:5432/corporate_actions_db
PROWESS_API_KEY=your_prowess_api_key_from_cmie

alembic upgrade head

python -m uvicorn api:app --reload --port 8000
`/api/v1/corporate-actions/today` | Events happening TODAY 
 `/api/v1/corporate-actions` | all events

 ***important***
 python daily_updater_new.py 
 this updates the db
