# Default .env location
ENV_FILE=.env

# Command: make up
up: check-env setup-dirs build init
	@echo "🚀 Starting Airflow stack with .env configuration..."
	docker compose up -d

# Command: make down
down:
	@echo "🛑 Stopping Airflow stack..."
	docker compose down

# Command: make restart
restart: down up

# Command: make logs
logs:
	docker compose logs -f

# Command: make test
test:
	@echo "🧪 Running pytest data quality checks..."
	docker compose exec airflow-worker pytest tests/

# Build Airflow images
build:
	@echo "🔨 Building Airflow images..."
	docker compose build

# Initialize Airflow (safe to rerun)
init:
	@echo "⚙️ Initializing Airflow database and users..."
	docker compose run --rm airflow-init

# Ensure required folders exist with correct ownership
setup-dirs:
	@echo "📂 Creating and fixing permissions for data/ and output/..."
	mkdir -p data output
	sudo chown -R 50000:0 data output
	sudo chmod -R 775 data output

# Ensure .env exists
check-env:
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "⚙️  .env not found, creating one..."; \
		read -p 'Enter your OpenWeather API key: ' APIKEY; \
		echo "AIRFLOW_UID=50000" > $(ENV_FILE); \
		echo "AIRFLOW_GID=0" >> $(ENV_FILE); \
		echo "OPENWEATHER_API_KEY=$$APIKEY" >> $(ENV_FILE); \
		echo ".env file created ✅"; \
	else \
		echo "✅ Using existing .env"; \
	fi