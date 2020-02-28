FROM python:3.8-slim


ENV APP_HOME /app
WORKDIR $APP_HOME

# Install dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY . .

# Run the application
ENTRYPOINT hypercorn main:app --bind 0.0.0.0:${PORT}
